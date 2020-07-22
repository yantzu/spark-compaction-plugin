package com.heytap.bdc.compaction

import java.io.IOException
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Locale}

import com.google.common.base.Predicates
import com.google.common.collect.Collections2
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.mapred.JobID
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.{JobContext, OutputCommitter, TaskAttemptContext, TaskID, TaskType}
import org.apache.spark.TaskContext
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

class CompactFilesCommitProtocol(jobId: String,
                                 path: String,
                                 dynamicPartitionOverwrite: Boolean = false)
  extends SQLHadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite) {

  private val DEFAULT_IN_PROGRESS_PREFIX = "_"
  private val DEFAULT_IN_PROGRESS_SUFFIX = ".inprogress"
  
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
  private def getAllCommittedTaskPaths(context: JobContext): (FileSystem, Path, Array[FileStatus]) = {
    val jobAttemptPath = fileOutputCommitter.getJobAttemptPath(context)
    val fileSystem = jobAttemptPath.getFileSystem(context.getConfiguration)
    val committedTaskPaths = fileSystem.listStatus(jobAttemptPath, new CommittedTaskFilter())
    (fileSystem, jobAttemptPath, committedTaskPaths)
  }

  private def getOutputFileFormat(fileSystem: FileSystem, committedTaskPaths: Array[FileStatus]): Option[Compactor.FileFormat.FileFormat] = {
    for (stat <- committedTaskPaths) {
      if (stat.isFile) {
        if (stat.getLen > CompactFilesCommitProtocol.MinFileSize) {
          val inputStream = fileSystem.open(stat.getPath)
          val buffer = new Array[Byte](CompactFilesCommitProtocol.MinFileSize)
          inputStream.readFully(buffer)
          val magic = new String(buffer)
          if (magic.startsWith(CompactFilesCommitProtocol.RcMagic)) {
            return Some(Compactor.FileFormat.RC)
          } else if (magic.startsWith(CompactFilesCommitProtocol.OrcMagic)) {
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

  private def getAllGroupedCommittedLeafDirectoryPath(sparkSession: SparkSession,
                                                      fileSystem: FileSystem,
                                                      committedTaskPaths: Array[FileStatus],
                                                      parallelism: Int)
        : mutable.MultiMap[String, (String, Array[(String, Long)])] = {
    //[String, mutable.Set[(String, Array[(String, Long)])]] => [hivePartitionKey, mutable.Set[(hivePartitionPathOfTask, Array[(filePath, fileLength)])]]
    val result =
      new mutable.HashMap[String, mutable.Set[(String, Array[(String, Long)])]] with mutable.MultiMap[String, (String, Array[(String, Long)])]

    //parallelism in executor
    val committedTaskDirRdd = sparkSession.sparkContext.parallelize(committedTaskPaths.map(_.getPath.toString), parallelism)
    val serializableHadoopConf = new SerializableConfiguration(fileSystem.getConf)

    val ret = new Array[mutable.HashMap[String, mutable.Set[(String, Array[(String, Long)])]]](committedTaskDirRdd.partitions.length)
    sparkSession.sparkContext.runJob(committedTaskDirRdd,    
      (taskContext: TaskContext, iter: Iterator[String]) => {
        getAllGroupedCommittedLeafDirectoryPathOfTask(serializableHadoopConf.value, iter)
      },
      committedTaskDirRdd.partitions.indices,
      (index, res: mutable.HashMap[String, mutable.Set[(String, Array[(String, Long)])]]) => {
        ret(index) = res
      })

    ret.foreach(committedLeafDirectoryPathOfTask => {
      if (committedLeafDirectoryPathOfTask != null) {
        for (committedLeafDirectoryPathKey <- committedLeafDirectoryPathOfTask.keys) {
          val committedLeafDirectoryPathGroup = committedLeafDirectoryPathOfTask.get(committedLeafDirectoryPathKey).get
          for (committedLeafDirectoryPath <- committedLeafDirectoryPathGroup) {
            result.addBinding(committedLeafDirectoryPathKey,
              (committedLeafDirectoryPath._1, committedLeafDirectoryPath._2))
          }
        }
      }
    })

    result
  }

  private def getAllGroupedCommittedLeafDirectoryPathOfTask(configuration: Configuration,
                                                            iter: Iterator[String])
      : mutable.HashMap[String, mutable.Set[(String, Array[(String, Long)])]] = {
    //[String, mutable.Set[(String, Array[(String, Long)])]] => [hivePartitionKey, mutable.Set[(hivePartitionPathOfTask, Array[(filePath, fileLength)])]]
    val result = new mutable.HashMap[String, mutable.Set[(String, Array[(String, Long)])]] with mutable.MultiMap[String, (String, Array[(String, Long)])]
    iter.map((committedTaskDir: String) => {
      val committedTaskPath = new Path(committedTaskDir)
      val fileSystem = committedTaskPath.getFileSystem(configuration)
      val committedTaskFileStatus = fileSystem.getFileStatus(committedTaskPath)
      if (committedTaskFileStatus.isFile) {
        throw new IllegalStateException(committedTaskDir + "should be a directory rather than a file")
      } else if (committedTaskFileStatus.isDirectory) {
        getAllGroupedCommittedLeafDirectoryPathOfTask(fileSystem, fileSystem.listStatus(committedTaskPath), committedTaskFileStatus)
      } else {
        //ignore symlink case
        null
      }
    }).filter(_ != null).foreach((committedLeafDirectoryPathOfTask: mutable.MultiMap[String, (String, Array[(String, Long)])]) => {
      for (committedLeafDirectoryPathKey <- committedLeafDirectoryPathOfTask.keys) {
        val committedLeafDirectoryPathGroup = committedLeafDirectoryPathOfTask.get(committedLeafDirectoryPathKey).get
        for (committedLeafDirectoryPath <- committedLeafDirectoryPathGroup) {
          result.addBinding(committedLeafDirectoryPathKey,
            (committedLeafDirectoryPath._1, committedLeafDirectoryPath._2))
        }
      }
    })
    result
  }

  private def getAllGroupedCommittedLeafDirectoryPathOfTask(fileSystem: FileSystem,
                                                            committedTaskPaths: Array[FileStatus],
                                                            committedTaskPath: FileStatus,
                                                            parentResult: mutable.MultiMap[String, (String, Array[(String, Long)])] = null)
        : mutable.MultiMap[String, (String, Array[(String, Long)])] = {
    if (committedTaskPath == null) {
      throw new IllegalStateException("committedTaskPath could not be null")
    }

    val result = if (parentResult == null) {
      new mutable.HashMap[String, mutable.Set[(String, Array[(String, Long)])]] with mutable.MultiMap[String, (String, Array[(String, Long)])]
    } else {
      parentResult
    }

    breakable {
      for (stat <- committedTaskPaths) {
        if (stat.isFile) {
          val parentDirectory = stat.getPath.getParent
          val leafPathKey = parentDirectory.toString.replaceFirst(committedTaskPath.getPath.toString, "")
          val slashedLeafPathKey = if (leafPathKey.startsWith("/")) {
            leafPathKey
          } else {
            "/" + leafPathKey
          }
          //ensure all siblings are files
          committedTaskPaths.foreach(stat => {
            if (!stat.isFile) {
              throw new IllegalStateException(stat.getPath + " should be a file")
            }
          })
          result.addBinding(slashedLeafPathKey, (parentDirectory.toString, committedTaskPaths.map(stat => (stat.getPath.toString, stat.getLen))))
          break()
        } else if (stat.isDirectory) {
          getAllGroupedCommittedLeafDirectoryPathOfTask(fileSystem, fileSystem.listStatus(stat.getPath), committedTaskPath, result)
        } //ignore symlink case
      }
    }
    result
  }

  private def getAllCompactTaskSlips(committedLeafDirectoryPathGroups: mutable.MultiMap[String, (String, Array[(String, Long)])], 
                                     compactSize: Long, smallfileSize: Long): Seq[(Array[String], String)] = {
    if (smallfileSize >= compactSize) {
      throw new IllegalArgumentException("spark.compact.smallfile.size could not larger than spark.compact.size")
    }
    
    val result = new ListBuffer[(Array[String], String)]()

    for (committedLeafDirectoryPathKey <- committedLeafDirectoryPathGroups.keys) {
      val committedLeafDirectoryPathGroup = committedLeafDirectoryPathGroups.get(committedLeafDirectoryPathKey).get
      result ++= getCompactTaskSlips(committedLeafDirectoryPathKey,
        committedLeafDirectoryPathGroup, compactSize, smallfileSize)
    }

    result
  }

  private def getCompactTaskSlips(committedLeafDirectoryPathKey: String,
                                  committedLeafDirectoryPathGroup: mutable.Set[(String, Array[(String, Long)])],
                                  compactSize: Long, smallfileSize: Long): Seq[(Array[String], String)] = {
    val result = new ListBuffer[(Array[String], String)]()
    
    var slipFiles = new ListBuffer[String]()
    var slipTotalSize: Long = 0
    //files in path of one same group should be compact together
    for (committedLeafDirectoryPath <- committedLeafDirectoryPathGroup) {
      for (committedLeafFilePath <- committedLeafDirectoryPath._2) {
        val filePath = committedLeafFilePath._1
        val fileLength = committedLeafFilePath._2
        if (fileLength < smallfileSize) {
          slipFiles += filePath
          slipTotalSize = slipTotalSize + fileLength
          if (slipTotalSize >= compactSize) {
            result += Tuple2(slipFiles.toArray,
              getCompactFileRelativePath(committedLeafDirectoryPathKey, slipFiles))

            slipFiles = new ListBuffer[String]()
            slipTotalSize = 0
          }
        }
      }
    }
    if (slipTotalSize > 0 && slipFiles.length > 1) {
      result += Tuple2(slipFiles.toArray,
        getCompactFileRelativePath(committedLeafDirectoryPathKey, slipFiles))
    }

    result
  }

  private def getCompactFileRelativePath(committedLeafDirectoryPathKey: String, slipFiles: ListBuffer[String]): String = {
    val headPath = new Path(slipFiles.head)
    //val parentPath = headPath.getParent
    
    val compactFileName = headPath.getName.replaceFirst("part", "compact")
    return new Path(committedLeafDirectoryPathKey, compactFileName).toString
  }

  private def compactFiles(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    val (fs, jobAttemptPath, committedTaskPaths) = getAllCommittedTaskPaths(jobContext)
    val fileFormat = getOutputFileFormat(fs, committedTaskPaths)
    if (fileFormat.isDefined) {
      logInfo("Detect compact file format:" + fileFormat.get)
    } else {
      logWarning("Not able to detect file format, bypass small file compact")
      return
    }
    
    val sparkSession: SparkSession = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)
    val compactPlanParallelism = sparkSession.conf.get("spark.compact.plan.parallelism", "5").toInt

    val committedLeafDirectoryPathGroups = getAllGroupedCommittedLeafDirectoryPath(sparkSession, fs, committedTaskPaths, compactPlanParallelism)
    val jobAttemptPathString = jobAttemptPath.toString
    for (committedLeafDirectoryPathGroup <- committedLeafDirectoryPathGroups.values) {
      logInfo("Detect compact directory group: " +
        committedLeafDirectoryPathGroup.map(_._1.toString.replace(jobAttemptPathString, "")).mkString("[", ", ", "]"))
    }
    
    val compactSize = sparkSession.conf.get("spark.compact.size", (1024 * 1024 * 1024).toString).toLong
    val smallfileSize = sparkSession.conf.get("spark.compact.smallfile.size", (32 * 1024 * 1024).toString).toLong
    val compactTaskSlips = getAllCompactTaskSlips(committedLeafDirectoryPathGroups, compactSize, smallfileSize)
    if (compactTaskSlips.length == 0) {
      logInfo("compactTaskSlips is empty, no compact tasks required")
      return
    }
    val compactTaskSlipRdd = sparkSession.sparkContext.parallelize(compactTaskSlips, compactTaskSlips.length)

    val serializableHadoopConf = new SerializableConfiguration(jobContext.getConfiguration)
    val compactWorkingRoot = jobAttemptPath.toString
    val beforeCompactTaskIds = util.Arrays.asList(fs.listStatus(jobAttemptPath).map(_.getPath.getName) : _*) //include _temporary actually
    val successTaskIds: Array[String] = sparkSession.sparkContext.runJob(compactTaskSlipRdd,
      (taskContext: TaskContext, iter: Iterator[(Array[String], String)]) => {
        val jobTrackerID = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(new Date)
        val jobId = new JobID(jobTrackerID, taskContext.stageId())
        val taskId = new TaskID(jobId, TaskType.MAP, taskContext.partitionId())
        //val taskAttemptId = new TaskAttemptID(taskId, taskContext.attemptNumber())

        iter.foreach((compactTask: (Array[String], String)) => {
          val compactToTargetFilePath = new Path(compactTask._2)
          val compactToTempFileName = new Path(compactToTargetFilePath.getParent, 
            DEFAULT_IN_PROGRESS_PREFIX + compactToTargetFilePath.getName + "." + taskContext.attemptNumber() + DEFAULT_IN_PROGRESS_SUFFIX).toString
          val compactToTempFile = new Path(compactWorkingRoot, taskId.toString) + compactToTempFileName
          val compactToFile = new Path(compactWorkingRoot, taskId.toString) + compactTask._2
          Compactor.getCompactor(fileFormat.get).compact(serializableHadoopConf.value, compactTask._1, compactToTempFile, compactToFile)
        })

        taskId.toString
      })

    val afterCompactTaskIds = util.Arrays.asList(fs.listStatus(jobAttemptPath).map(_.getPath.getName): _*) //include _temporary actually
    val compactTaskIds = Collections2.filter(afterCompactTaskIds, Predicates.not(Predicates.in(beforeCompactTaskIds)))

    import scala.collection.JavaConverters._
    logInfo("Clean temp files")
    compactTaskIds.asScala.foreach(taskId => {
      val taskWorkingDirectory = new Path(compactWorkingRoot, taskId)
      if (fs.exists(taskWorkingDirectory)) {
        cleanTempFiles(fs, taskWorkingDirectory)
      }
    })
    
    logInfo("Compacted " + compactTaskSlips.map(_._1.length).sum + " files to " + compactTaskSlips.length + " files")
  }

  private def cleanTempFiles(fs: FileSystem, path: Path): Unit = {
    fs.listStatus(path).foreach(fileStatus => {
      if (fileStatus.isDirectory) {
        cleanTempFiles(fs, fileStatus.getPath)
      } else if (fileStatus.isFile) {
        val fileName = fileStatus.getPath.getName
        if (fileName.startsWith(DEFAULT_IN_PROGRESS_PREFIX)
          && fileName.endsWith(DEFAULT_IN_PROGRESS_SUFFIX)) {
          logInfo("Delete " + fileStatus.getPath)
          fs.delete(fileStatus.getPath, false)
        }
      }
    })
  }

  private class CommittedTaskFilter extends PathFilter {
    override def accept(path: Path): Boolean = !(FileOutputCommitter.PENDING_DIR_NAME == path.getName)
  }
}

object CompactFilesCommitProtocol {
  private val MinFileSize: Int = 4

  private val RcMagic: String = "RCF"
  private val OrcMagic: String = "ORC"
  private val ParquetMagic: String = "PAR1"
}