package com.heytap.bdc.compaction

import java.io.IOException

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.{JobContext, OutputCommitter, TaskAttemptContext}
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.internal.io.HadoopMapReduceCommitProtocol
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

class CompactFilesCommitProtocol(jobId: String,
                                 path: String,
                                 dynamicPartitionOverwrite: Boolean = false)
  extends HadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite) {

  @transient private var fileOutputCommitter: FileOutputCommitter = _
  
  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    compactFiles(jobContext, taskCommits)
    super.commitJob(jobContext, taskCommits)
  }

  //overwrite setupCommitter in order to get committer
  override protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    val outputCommitter: OutputCommitter = super.setupCommitter(context)
    if (outputCommitter.isInstanceOf[FileOutputCommitter]) {
      fileOutputCommitter = outputCommitter.asInstanceOf[FileOutputCommitter]
    } else {
      throw new IllegalStateException("Cannot compact small files of " + outputCommitter.getClass)
    }
    return outputCommitter;
  }

  @throws[IOException]
  private def getAllCommittedTaskPaths(context: JobContext): (FileSystem, Array[FileStatus]) = {
    val jobAttemptPath = fileOutputCommitter.getJobAttemptPath(context)
    val fileSystem = jobAttemptPath.getFileSystem(context.getConfiguration)
    val committedTaskPaths = fileSystem.listStatus(jobAttemptPath, new CommittedTaskFilter())
    (fileSystem, committedTaskPaths)
  }

  private def getOutputFileFormat(fileSystem: FileSystem, committedTaskPaths: Array[FileStatus]): Option[Compactor.FileFormat.FileFormat] = {
    for (stat <- committedTaskPaths) {
      if (stat.isFile) {
        if (stat.getLen > CompactFilesCommitProtocol.MinFileSize) {
          val inputStream = fileSystem.open(stat.getPath)
          val buffer = new Array[Byte](CompactFilesCommitProtocol.MinFileSize)
          inputStream.readFully(buffer)
          val magic = new String(buffer)
          if (magic.startsWith(CompactFilesCommitProtocol.OrcMagic)) {
            return Some(Compactor.FileFormat.ORC)
          } else if (magic.startsWith(CompactFilesCommitProtocol.ParquetMagic)) {
            return Some(Compactor.FileFormat.Parquet)
          }
        }
      } else if (stat.isDirectory) {
        val subResult = getOutputFileFormat(fileSystem, fileSystem.listStatus(stat.getPath))
        if (subResult.isDefined) {
          return subResult
        }
      } //ignore symlink case
    }
    return None
  }

  private def getAllGroupedCommittedLeafDirectoryPath(fileSystem: FileSystem,
                                       committedTaskPaths: Array[FileStatus],
                                       committedTaskPath: FileStatus = null,
                                       parentResult: mutable.MultiMap[String, Path] = null): mutable.MultiMap[String, Path] = {
    val result = if (parentResult == null) {
      new mutable.HashMap[String, mutable.Set[Path]] with mutable.MultiMap[String, Path]
    } else {
      parentResult
    }

    breakable {
      for (stat <- committedTaskPaths) {
        if (stat.isFile) {
          if (committedTaskPath == null) {
            throw new IllegalStateException(stat.getPath.toString + "should be a directory rather than a file")
          } else {
            val parentDirectory = stat.getPath.getParent
            val leafPathKey = parentDirectory.toString.replaceFirst(committedTaskPath.getPath.toString, "")
            result.addBinding(leafPathKey, parentDirectory)
            break()
          }
        } else if (stat.isDirectory) {
          if (committedTaskPath == null) {
            getAllGroupedCommittedLeafDirectoryPath(fileSystem, fileSystem.listStatus(stat.getPath), stat, result)
          } else {
            getAllGroupedCommittedLeafDirectoryPath(fileSystem, fileSystem.listStatus(stat.getPath), committedTaskPath, result)
          }
        } //ignore symlink case
      }
    }
    result
  }

  private def getAllCompactTaskSlips(fileSystem: FileSystem,
                                    committedLeafDirectoryPathGroups: mutable.MultiMap[String, Path],
                                    compactSize: Long, smallfileSize: Long): Array[(Array[String], String)] = {
    if (smallfileSize >= compactSize) {
      throw new IllegalArgumentException("spark.compact.smallfile.size could not larger than spark.compact.size")
    }
    
    val result = new ListBuffer[(Array[String], String)]()
    
    for (committedLeafDirectoryPathGroup <- committedLeafDirectoryPathGroups.values) {
      var slipFiles = new ListBuffer[Path]()
      var slipTotalSize: Long = 0
      //files in path of one same group should be compact together
      for (committedLeafDirectoryPath <- committedLeafDirectoryPathGroup) {
        for (committedLeafFilePath <- fileSystem.listStatus(committedLeafDirectoryPath)) {
          if (!committedLeafFilePath.isFile) {
            throw new IllegalStateException(committedLeafFilePath.toString + " should be a file")
          }
          if (committedLeafFilePath.getLen < smallfileSize) {
            slipFiles += committedLeafFilePath.getPath
            slipTotalSize = slipTotalSize + committedLeafFilePath.getLen
            if (slipTotalSize >= compactSize) {
              result += Tuple2(slipFiles.map(_.toString).toArray, getCompactFilePath(slipFiles))

              slipFiles = new ListBuffer[Path]()
              slipTotalSize = 0
            }
          }
        }
      }
      if (slipTotalSize > 0 && slipFiles.length > 1) {
        result += Tuple2(slipFiles.map(_.toString).toArray, getCompactFilePath(slipFiles))
      }
    }

    result.toArray
  }

  private def getCompactFilePath(slipFiles: ListBuffer[Path]): String = {
    val headPath = slipFiles.head
    val parentPath = headPath.getParent
    val compactFileName = headPath.getName.replaceFirst("part", "compact")
    return new Path(parentPath, compactFileName).toString
  }

  private def compactFiles(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    val (fs, committedTaskPaths) = getAllCommittedTaskPaths(jobContext)
    val fileFormat = getOutputFileFormat(fs, committedTaskPaths)
    if (fileFormat.isEmpty) {
      throw new IllegalStateException("Unable to detect file format of files")
    } else {
      logInfo("Detect compact file format:" + fileFormat.get)
    }
    val committedLeafDirectoryPathGroups: mutable.MultiMap[String, Path] = getAllGroupedCommittedLeafDirectoryPath(fs, committedTaskPaths)
    for (committedLeafDirectoryPathGroup <- committedLeafDirectoryPathGroups.values) {
      logInfo("Detect compact directory group: " + committedLeafDirectoryPathGroup.mkString("[", ", ", "]"))
    }
    
    val sparkSession: SparkSession = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)
    val compactSize = sparkSession.conf.get("spark.compact.size", (1024 * 1024 * 1024).toString).toLong
    val smallfileSize = sparkSession.conf.get("spark.compact.smallfile.size", (16 * 1024 * 1024).toString).toLong

    val compactTaskSlips = getAllCompactTaskSlips(fs, committedLeafDirectoryPathGroups, compactSize, smallfileSize)
    val compactTaskSlipRdd = sparkSession.sparkContext.parallelize(compactTaskSlips, compactTaskSlips.length)

    val serializableHadoopConf = new SerializableConfiguration(jobContext.getConfiguration)
    sparkSession.sparkContext.runJob(compactTaskSlipRdd, (iter: Iterator[(Array[String], String)]) => {
      iter.foreach((compactTask: (Array[String], String)) => {
        Compactor.getCompactor(fileFormat.get).compact(serializableHadoopConf.value, compactTask._1, compactTask._2)
      })
    })
    logInfo("Compact " + compactTaskSlips.map(_._1.length).sum + " files to " + compactTaskSlips.length + " files")
  }

  private class CommittedTaskFilter extends PathFilter {
    override def accept(path: Path): Boolean = !(FileOutputCommitter.PENDING_DIR_NAME == path.getName)
  }
}

object CompactFilesCommitProtocol {
  private val MinFileSize: Int = 4

  private val OrcMagic: String = "ORC"
  private val ParquetMagic: String = "PAR1"
}