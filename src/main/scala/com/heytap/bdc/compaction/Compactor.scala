package com.heytap.bdc.compaction

import java.util

import com.google.common.base.{Preconditions, Predicates}
import com.google.common.collect.Collections2
import com.heytap.bdc.compaction.Compactor.FileFormat.FileFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.orc.OrcFile
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter, ParquetWriter}
import org.apache.parquet.hadoop.metadata.{BlockMetaData, FileMetaData, GlobalMetaData, ParquetMetadata}
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}
import org.apache.parquet.schema.MessageType

import scala.collection.JavaConversions._

trait Compactor extends Serializable with Logging {
  def compact(conf: Configuration, compactFrom: Array[String], compactTo: String) = {
    info("Compact files " + compactFrom.mkString("[", ",", "]") + " to " + compactTo)
    val compactToPath = new Path(compactTo)
    val fileSystem = compactToPath.getFileSystem(conf)
    if (fileSystem.exists(compactToPath)) {
      warn(compactTo + " already exist, won't compact")
    } else {
      val compactToTemp = compactTo + ".in-progress"
      val compactToTempPath = new Path(compactToTemp)
      if(fileSystem.exists(compactToTempPath)) {
        warn(compactToTempPath + " exist, delete and re-compact ")
      }
      compactInternal(conf, compactFrom, compactToTemp)
      fileSystem.rename(compactToTempPath, compactToPath)
    }
    info("Delete compact source files " + compactFrom.mkString("[", ",", "]"))
    for (file <- compactFrom) {
      val path = new Path(file)
      if(fileSystem.exists(path)) {
        val deleteResult = fileSystem.delete(path, false)
        if (!deleteResult) {
          throw new IllegalStateException("Failed to delete file " + file)
        }
      }
    }
  }

  protected def compactInternal(conf: Configuration, compactFrom: Array[String], compactTo: String)
}

class OrcCompactor extends Compactor {
  override protected def compactInternal(conf: Configuration, compactFrom: Array[String], compactTo: String): Unit = {
    val writeOptions = OrcFile.writerOptions(conf)
    val compactFromPaths = util.Arrays.asList(compactFrom.map(new Path(_)): _*)
    val successCompactFromPaths = OrcFile.mergeFiles(new Path(compactTo), writeOptions, compactFromPaths)
    if (successCompactFromPaths.size() != compactFrom.length) {
      val failedCompactFromPaths = Collections2.filter(compactFromPaths, Predicates.not(Predicates.in(successCompactFromPaths)))
      throw new IllegalStateException("Merge ORC failed due to files cannot be merged:" + failedCompactFromPaths)
    }
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
    if (fileFormat == FileFormat.ORC) {
      return new OrcCompactor()
    } else if (fileFormat == FileFormat.Parquet) {
      return new ParquetCompactor()
    }
    throw new IllegalArgumentException("Not supported file format:" + fileFormat)
  }

  object FileFormat extends Enumeration {
    type FileFormat = Value
    val ORC = Value("ORC")
    val Parquet = Value("Parquet")
  }
}



