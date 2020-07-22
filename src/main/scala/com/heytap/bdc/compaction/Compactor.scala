package com.heytap.bdc.compaction

import java.io.FileNotFoundException
import java.util

import com.google.common.base.{Preconditions, Predicates}
import com.google.common.collect.Collections2
import com.heytap.bdc.compaction.Compactor.FileFormat.FileFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.io.{RCFile, RCFileOutputFormat}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.ipc.RemoteException
import org.apache.orc.OrcFile
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter, ParquetWriter}
import org.apache.parquet.hadoop.metadata.{BlockMetaData, FileMetaData, GlobalMetaData, ParquetMetadata}
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}
import org.apache.parquet.schema.MessageType

import scala.collection.JavaConversions._

trait Compactor extends Serializable with Logging {
  def compact(conf: Configuration, compactFromFiles: Array[String],
              compactToTempFile: String, compactToFile: String): Unit = {
    info("Compacting\nFrom: " + compactFromFiles.mkString("[", ",", "]") + "\nTo:" + compactToFile + "\nTemp: " + compactToTempFile)
    val compactToPath = new Path(compactToFile)
    val fileSystem = compactToPath.getFileSystem(conf)
    if (fileSystem.exists(compactToPath)) {
      warn(compactToFile + " already exist, won't compact")
      //won't throw exception so can continue delete source files
    } else {
      val compactToTempPath = new Path(compactToTempFile)
      if(fileSystem.exists(compactToTempPath)) {
        warn(compactToTempPath + " exist, delete and re-compact ")
        fileSystem.delete(compactToTempPath, false)
      }

      var compactSucceed = false
      try {
        compactInternal(conf, compactFromFiles, compactToTempFile)
        compactSucceed = true
      } catch {
        case re: RemoteException => {
          fileSystem.delete(compactToTempPath, false)
          if (re.getClassName.equals("java.io.FileNotFoundException")
            && !re.getMessage.contains(compactToTempFile) && fileSystem.exists(compactToPath)) {
            warn("Compact failed, should be another attempt has succeed, delete temp file, message:" + re.toString)
            //won't throw exception so can continue delete source files
          } else {
            throw re
          }
        }
        case fnfe: FileNotFoundException => {
          fileSystem.delete(compactToTempPath, false)
          if (!fnfe.getMessage.contains(compactToTempFile) && fileSystem.exists(compactToPath)) {
            warn("Compact failed, should be another attempt has succeed, delete temp file, message:" + fnfe.toString)
            //won't throw exception so can continue delete source files
          }
        }
        case e: Exception => {
          fileSystem.delete(compactToTempPath, false)
          throw e
        }
      }
      
      try {
        if(compactSucceed) {
          fileSystem.rename(compactToTempPath, compactToPath)
        }
      } catch {
        case e: Exception => {
          fileSystem.delete(compactToTempPath, false)
          if (fileSystem.exists(compactToPath)) {
            info(compactToFile + " already exist, should be another attempt has succeed, delete temp file")
            //won't throw exception so can continue delete source files
          } else {
            throw e
          }
        }
      }
    }

    for (file <- compactFromFiles) {
      val path = new Path(file)
      try {
        info("Delete compact source file " + path)
        val deleteSucceed = fileSystem.delete(path, false)
        if (!deleteSucceed && fileSystem.exists(path)) {
          throw new IllegalStateException("Failed to delete file " + file)
        }
        //delete empty ancestor directory
        var parentPath = path.getParent
        var siblingStatuses = fileSystem.listStatus(parentPath)
        while (siblingStatuses.length == 0) {
          info("Delete empty source directory " + parentPath)
          fileSystem.delete(parentPath, true)
          parentPath = parentPath.getParent
          siblingStatuses = fileSystem.listStatus(parentPath)
        }
      } catch {
        case e: Exception => {
          if (fileSystem.exists(path)) {
            throw e
          }
        }
      }
    }
  }

  protected def compactInternal(conf: Configuration, compactFrom: Array[String], compactTo: String)
}

class RcCompactor extends Compactor {
  override protected def compactInternal(conf: Configuration, compactFrom: Array[String], compactTo: String): Unit = {
    var compactToCodec: CompressionCodec = null
    var compactToColumnNumber = -1
    var compactToWriter: RCFile.Writer = null
    
    val compactToPath = new Path(compactTo)
    val fileSystem = compactToPath.getFileSystem(conf)

    for (compactFromPart <- compactFrom) {
      val compactFromPath = new Path(compactFromPart)
      val compactFromReader = new RCFile.Reader(fileSystem, compactFromPath, conf)
      while (compactFromReader.nextBlock()) {
        //key part
        val keyBuffer = compactFromReader.getCurrentKeyBufferObj
        val recordLength = compactFromReader.getCurrentBlockLength
        val keyLength = compactFromReader.getCurrentKeyLength
        val compressedKeyLength = compactFromReader.getCurrentCompressedKeyLen
        val codec = compactFromReader.getCompressionCodec

        //value part
        val valueBuffer = compactFromReader.getCurrentValueBufferObj

        if (compactToWriter == null) {
          compactToCodec = codec
          compactToColumnNumber = keyBuffer.getColumnNumber()

          RCFileOutputFormat.setColumnNumber(conf, keyBuffer.getColumnNumber)
          compactToWriter = new RCFile.Writer(fileSystem, conf, compactToPath, null, codec)
        }

        val sameCodec = ((codec == compactToCodec)) || codec.getClass.equals(compactToCodec.getClass)
        if ((keyBuffer.getColumnNumber != compactToColumnNumber) || (!sameCodec)) {
          throw new IllegalStateException("RCFileMerge failed because the input files" 
            + " use different CompressionCodec or have different column number" 
            + " setting.")
        }

        compactToWriter.flushBlock(keyBuffer, valueBuffer, recordLength, keyLength, compressedKeyLength)
      }
    }

    if (compactToWriter != null) {
      compactToWriter.close()
    }
  }
}

class OrcCompactor extends Compactor {
  override protected def compactInternal(conf: Configuration, compactFrom: Array[String], compactTo: String): Unit = {
    val writeOptions = OrcFile.writerOptions(conf)
    val compactFromPaths = util.Arrays.asList(compactFrom.map(new Path(_)): _*)
    val successCompactFromPaths = OrcFile.mergeFiles(new Path(compactTo), writeOptions, compactFromPaths)
    if (successCompactFromPaths.size() != compactFrom.length) {
      val failedCompactFromPaths = Collections2.filter(compactFromPaths, Predicates.not(Predicates.in(successCompactFromPaths)))
      val allFilesEmpty = isAllFilesEmpty(conf, failedCompactFromPaths)
      if (!allFilesEmpty) {
        throw new IllegalStateException("Merge ORC failed due to files cannot be merged:" + failedCompactFromPaths)
      }
    }
  }

  private def isAllFilesEmpty(conf: Configuration, failedCompactFromPaths: util.Collection[Path]): Boolean = {
    val readOptions = OrcFile.readerOptions(conf)
    for (failedCompactFromPath <- failedCompactFromPaths) {
      val reader = OrcFile.createReader(failedCompactFromPath, readOptions)
      val numberOfRows = reader.getStripes.map(_.getNumberOfRows).sum
      if (numberOfRows != 0) {
        return false
      }
    }
    return true
  }
}

class ParquetCompactor extends Compactor {
  override protected def compactInternal(conf: Configuration, compactFrom: Array[String], compactTo: String): Unit = {
    val compactFromPaths = util.Arrays.asList(compactFrom.map(new Path(_)): _*)
    val mergedMeta = mergeMetadataFiles(compactFromPaths, conf).getFileMetaData

    val writer = new ParquetFileWriter(HadoopOutputFile.fromPath(new Path(compactTo), conf),
      mergedMeta.getSchema, ParquetFileWriter.Mode.CREATE,
      ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.MAX_PADDING_SIZE_DEFAULT)
    writer.start()
    for (input <- compactFromPaths) {
      writer.appendFile(HadoopInputFile.fromPath(input, conf))
    }
    writer.end(mergedMeta.getKeyValueMetaData)
  }

  private def mergeMetadataFiles(files: util.List[Path], conf: Configuration): ParquetMetadata = {
    if (files.isEmpty) {
      throw new IllegalArgumentException("Cannot merge an empty list of metadata")
    }
    var globalMetaData: GlobalMetaData = null
    val blocks = new util.ArrayList[BlockMetaData]

    for (p <- files) {
      val file = HadoopInputFile.fromPath(p, conf)
      val options = HadoopReadOptions.builder(conf).withMetadataFilter(ParquetMetadataConverter.NO_FILTER).build
      val reader = new ParquetFileReader(file, options)
      val pmd = reader.getFooter
      val fmd = reader.getFileMetaData
      globalMetaData = mergeInto(fmd, globalMetaData, true)
      blocks.addAll(pmd.getBlocks)
    }
    // collapse GlobalMetaData into a single FileMetaData, which will throw if they are not compatible
    return new ParquetMetadata(globalMetaData.merge, blocks)
  }
  
  private def mergeInto(toMerge: FileMetaData, mergedMetadata: GlobalMetaData, strict: Boolean): GlobalMetaData = {
    var schema: MessageType = null
    val newKeyValues = new util.HashMap[String, util.Set[String]]()
    val createdBy = new util.HashSet[String]()
    if (mergedMetadata != null) {
      schema = mergedMetadata.getSchema()
      newKeyValues.putAll(mergedMetadata.getKeyValueMetaData)
      createdBy.addAll(mergedMetadata.getCreatedBy)
    }
    if ((schema == null && toMerge.getSchema != null) || (schema != null && !schema.equals(toMerge.getSchema))) {
      schema = mergeInto(toMerge.getSchema, schema, strict)
    }
    for (entry <- toMerge.getKeyValueMetaData.entrySet) {
      var values = newKeyValues.get(entry.getKey)
      if (values == null) {
        values = new util.LinkedHashSet[String]()
        newKeyValues.put(entry.getKey, values)
      }
      values.add(entry.getValue)
    }
    createdBy.add(toMerge.getCreatedBy)
    return new GlobalMetaData(schema, newKeyValues, createdBy)
  }
  
  private def mergeInto(toMerge: MessageType, mergedSchema: MessageType, strict: Boolean): MessageType = {
    if (mergedSchema == null) {
      return toMerge
    }
    return mergedSchema.union(toMerge, strict)
  }
}

object Compactor extends Serializable {

  def getCompactor(fileFormat: FileFormat): Compactor = {
    if (fileFormat == FileFormat.RC) {
      return new RcCompactor()
    } else if (fileFormat == FileFormat.ORC) {
      return new OrcCompactor()
    } else if (fileFormat == FileFormat.Parquet) {
      return new ParquetCompactor()
    }
    throw new IllegalArgumentException("Not supported file format:" + fileFormat)
  }

  object FileFormat extends Enumeration {
    type FileFormat = Value
    val RC = Value("RC")
    val ORC = Value("ORC")
    val Parquet = Value("Parquet")
  }
}



