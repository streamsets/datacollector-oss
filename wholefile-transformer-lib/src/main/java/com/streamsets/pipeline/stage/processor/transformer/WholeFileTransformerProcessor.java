/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.transformer;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.converter.AvroParquetConstants;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.lib.util.AvroToParquetConverterUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.Strings;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WholeFileTransformerProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(WholeFileTransformerProcessor.class);
  private static final String FILENAME = "filename";

  private final JobConfig jobConfig;
  private ErrorRecordHandler errorRecordHandler;
  private Processor.Context context;

  private ParquetWriter parquetWriter;
  private ELEval compressionElEval;
  private ELEval rateLimitElEval;
  private ELEval tempDirElEval;
  private ELVars variables;


  public WholeFileTransformerProcessor(JobConfig jobConfig) {
    this.jobConfig = jobConfig;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    if (Strings.isNullOrEmpty(jobConfig.tempDir)) {
      issues.add(getContext().createConfigIssue(
          Groups.JOB.name(),
          JobConfig.TEMPDIR,
          Errors.CONVERT_02
      ));
    }

    this.context = getContext();
    this.errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    tempDirElEval = context.createELEval("tempDir");
    compressionElEval = context.createELEval("compressionCodec");
    rateLimitElEval = FileRefUtil.createElEvalForRateLimit(getContext());

    variables = context.createELVars();

    return issues;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Path tempParquetFile = null;
    try {
      validateRecord(record);

      Map<String, Field> sourceFileMetaData = record.get(FileRefUtil.FILE_INFO_FIELD_PATH).getValueAsMap();
      if (sourceFileMetaData.get(FILENAME) == null) {
        throw new TransformerStageCheckedException(Errors.CONVERT_03, FileRefUtil.FILE_INFO_FIELD_PATH + "/" + FILENAME);
      }

      String sourceFileName = sourceFileMetaData.get(FILENAME).getValueAsString();
      tempParquetFile = getAndValidateTempFilePath(record, sourceFileName);

      InputStream is = getAvroInputStream(record);

      DataFileStream<GenericRecord> fileReader = getFileReader(is, sourceFileName);

      writeParquet(sourceFileName, fileReader, tempParquetFile);

      Map<String, Object> metadata = generateHeaderAttrs(tempParquetFile);

      // build parquet OutputStream
      FileRef newFileRef = new WholeFileTransformerFileRef.Builder().filePath(tempParquetFile.toString()).bufferSize(
          jobConfig.wholeFileMaxObjectLen).rateLimit(FileRefUtil.evaluateAndGetRateLimit(rateLimitElEval,
          variables,
          jobConfig.rateLimit
      )).createMetrics(true).build();

      // move the file info field to source file info field
      Field fileInfo = record.get(FileRefUtil.FILE_INFO_FIELD_PATH);

      record.set(FileRefUtil.getWholeFileRecordRootField(newFileRef, metadata));
      record.set(FileRefUtil.WHOLE_FILE_SOURCE_FILE_INFO_PATH, fileInfo);

      batchMaker.addRecord(record);
    } catch (TransformerStageCheckedException ex) {
      if (tempParquetFile != null) {
        try {
          handleOldTempFiles(tempParquetFile);
        } catch (IOException ex1) {
          LOG.error("failed to delete temporary parquet file : {}", tempParquetFile.toString(), ex1);
        }
      }
      LOG.error(ex.getMessage(), ex.getParams(), ex);
      errorRecordHandler.onError(new OnRecordErrorException(record, ex.getErrorCode(), ex.getParams()));
    }
  }

  /**
   * Generate the Header attributes
   * @param file the {@link java.nio.file.Path} temporary parquet file path
   */
  private Map<String, Object> generateHeaderAttrs(Path file) throws StageException {
    try {
      Map<String, Object> recordHeaderAttr = new HashMap<>();
      recordHeaderAttr.put(HeaderAttributeConstants.FILE, file.toAbsolutePath());
      recordHeaderAttr.put(HeaderAttributeConstants.FILE_NAME, file.getFileName());
      recordHeaderAttr.put(HeaderAttributeConstants.SIZE, Files.size(file));
      recordHeaderAttr.put(HeaderAttributeConstants.LAST_MODIFIED_TIME, Files.getLastModifiedTime(file));

      return recordHeaderAttr;
    } catch (IOException e) {
      throw new TransformerStageCheckedException(Errors.CONVERT_09, e.toString(), e);
    }
  }

  /**
   * Validate the record is a whole file record
   * @param record the {@link com.streamsets.pipeline.api.Record} whole file record
   */
  private void validateRecord(Record record) throws StageException {
    try {
      FileRefUtil.validateWholeFileRecord(record);
    } catch (IllegalArgumentException e) {
      throw new TransformerStageCheckedException(Errors.CONVERT_01, e.toString(), e);
    }
  }

  /**
   * Return the temporary parquet file path and validate the path
   * @param record the {@link com.streamsets.pipeline.api.Record} whole file record
   * @param sourceFileName the source Avro file name
   */
  @VisibleForTesting
  Path getAndValidateTempFilePath(Record record, String sourceFileName) throws StageException {
    RecordEL.setRecordInContext(variables, record);
    String dirPath;
    try {
      dirPath = resolveEL(tempDirElEval, variables, jobConfig.tempDir, String.class);
    } catch (ELEvalException ex) {
      throw new TransformerStageCheckedException(Errors.CONVERT_04, jobConfig.tempDir);
    }

    if (Strings.isNullOrEmpty(dirPath)) {
      throw new TransformerStageCheckedException(Errors.CONVERT_02, jobConfig.tempDir);
    }

    if (Strings.isNullOrEmpty(sourceFileName)) {
      throw new TransformerStageCheckedException(Errors.CONVERT_03, FILENAME);
    }
    String fileName = jobConfig.uniquePrefix + sourceFileName + jobConfig.fileNameSuffix;
    Path tempParquetFile = Paths.get(dirPath, fileName);

    if (!tempParquetFile.isAbsolute()) {
      throw new TransformerStageCheckedException(Errors.CONVERT_05, tempParquetFile);
    }

    try {
      if (!Files.exists(tempParquetFile.getParent())) {
        Files.createDirectories(Paths.get(dirPath));
      }
    } catch (IOException ex) {
      throw new TransformerStageCheckedException(Errors.CONVERT_10, tempParquetFile.toString(), ex);
    }

    // handle old temp files
    try {
      handleOldTempFiles(tempParquetFile);
    } catch (IOException ex) {
      throw new TransformerStageCheckedException(Errors.CONVERT_06, tempParquetFile.toString(), ex);
    }

    return tempParquetFile;
  }

  /**
   * Delete temporary parquet file
   * @param tempParquetFile the {@link java.nio.file.Path} temporary parquet file path
   */
  private void handleOldTempFiles(Path tempParquetFile) throws IOException {
    if (tempParquetFile == null) {
      LOG.warn("temporary parquet file is empty");
      return;
    }
    Files.deleteIfExists(tempParquetFile);
  }

  /**
   * Return the Avro file input stream
   * @param record the {@link com.streamsets.pipeline.api.Record} whole file record
   */
  private InputStream getAvroInputStream(Record record) throws StageException {
    try {
      FileRef fileRef = record.get(FileRefUtil.FILE_REF_FIELD_PATH).getValueAsFileRef();

      // get avro reader
      final boolean includeChecksumInTheEvents = false;

      InputStream is = FileRefUtil.getReadableStream(
          getContext(),
          fileRef,
          InputStream.class,
          includeChecksumInTheEvents,
          null,
          null
      );

      return is;
    } catch (IOException ex) {
      throw new TransformerStageCheckedException(Errors.CONVERT_07, ex.toString(), ex);
    }
  }

  /**
   * Return the Avro file reader
   * @param is the {@link java.io.InputStream} input stream of the source Avro file
   * @param sourceFileName the source Avro file name
   */
  private DataFileStream<GenericRecord> getFileReader(InputStream is, String sourceFileName) throws StageException {
    try {
      DatumReader<GenericRecord> reader = new GenericDatumReader<>();
      DataFileStream<GenericRecord> fileReader = new DataFileStream<>(is, reader);
      return fileReader;
    } catch (IOException ex) {
      throw new TransformerStageCheckedException(Errors.CONVERT_11, sourceFileName, ex);
    }
  }

  /**
   * Convert Avro record to Parquet
   * @param sourceFileName the source Avro file name
   * @param fileReader the {@link org.apache.avro.file.DataFileStream} Avro file reader
   * @param tempParquetFile the {@link java.nio.file.Path} temporary parquet file path
   */
  private void writeParquet(String sourceFileName, DataFileStream<GenericRecord> fileReader, Path tempParquetFile) throws StageException {
    long recordCount = 0;
    GenericRecord avroRecord;
    Schema schema = fileReader.getSchema();

    LOG.debug("Start reading input file : {}", sourceFileName);
    try {
      // initialize parquet writer
      Configuration jobConfiguration = new Configuration();
      String compressionCodecName = compressionElEval.eval(variables, jobConfig.avroParquetConfig.compressionCodec, String.class);
      jobConfiguration.set(AvroParquetConstants.COMPRESSION_CODEC_NAME, compressionCodecName);
      jobConfiguration.setInt(AvroParquetConstants.ROW_GROUP_SIZE, jobConfig.avroParquetConfig.rowGroupSize);
      jobConfiguration.setInt(AvroParquetConstants.PAGE_SIZE, jobConfig.avroParquetConfig.pageSize);
      jobConfiguration.setInt(AvroParquetConstants.DICTIONARY_PAGE_SIZE, jobConfig.avroParquetConfig.dictionaryPageSize);
      jobConfiguration.setInt(AvroParquetConstants.MAX_PADDING_SIZE, jobConfig.avroParquetConfig.maxPaddingSize);

      // Parquet writer
      ParquetWriter.Builder builder = AvroToParquetConverterUtil.initializeWriter(
          new org.apache.hadoop.fs.Path(tempParquetFile.toString()),
          schema,
          jobConfiguration
      );
      parquetWriter = builder.build();

      while (fileReader.hasNext()) {
        avroRecord = fileReader.next();
        parquetWriter.write(avroRecord);
        recordCount++;
      }
      parquetWriter.close();

    } catch (IOException ex) {
      throw new TransformerStageCheckedException(
          Errors.CONVERT_08,
          sourceFileName,
          recordCount,
          ex
      );
    }
    LOG.debug("Finished writing {} records to {}", recordCount, tempParquetFile.getFileName());
  }

  private static <T> T resolveEL(ELEval elEval, ELVars elVars, String configValue, Class<T> returnType) throws
      ELEvalException {
    return elEval.eval(elVars, configValue, returnType);
  }
}
