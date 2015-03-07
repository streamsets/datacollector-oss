/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.RecordTarget;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.el.RecordEl;
import com.streamsets.pipeline.el.TimeEl;
import com.streamsets.pipeline.lib.generator.CharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.delimited.DelimitedCharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.text.TextCharDataGeneratorFactory;
import com.streamsets.pipeline.stage.destination.hdfs.writer.ActiveRecordWriters;
import com.streamsets.pipeline.stage.destination.hdfs.writer.RecordWriter;
import com.streamsets.pipeline.stage.destination.hdfs.writer.RecordWriterManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class HdfsTarget extends RecordTarget {
  private final static Logger LOG = LoggerFactory.getLogger(HdfsTarget.class);

  private final String hdfsUri;
  private final boolean hdfsKerberos;
  private final String kerberosPrincipal;
  private final String kerberosKeytab;
  private final Map<String, String> hdfsConfigs;
  private String uniquePrefix;
  private final String dirPathTemplate;
  private final String timeZoneID;
  private final String timeDriver;
  private final long maxRecordsPerFile;
  private final long maxFileSize;
  private final String compression;
  private final HdfsFileType fileType;
  private final String keyEl;
  private final HdfsSequenceFileCompressionType seqFileCompressionType;
  private final String lateRecordsLimit;
  private final LateRecordsAction lateRecordsAction;
  private final String lateRecordsDirPathTemplate;
  private final DataFormat dataFormat;
  private final CsvMode csvFileFormat;
  private final CsvHeader csvHeader;
  private final boolean csvReplaceNewLines;
  private final JsonMode jsonMode;
  private final String textFieldPath;
  private final boolean textEmptyLineIfNull;


  public HdfsTarget(String hdfsUri, boolean hdfsKerberos, String kerberosPrincipal, String kerberosKeytab,
      Map<String, String> hdfsConfigs, String uniquePrefix, String dirPathTemplate, String timeZoneID,
      String timeDriver, long maxRecordsPerFile, long maxFileSize, String compression,
      HdfsFileType fileType, String keyEl,
      HdfsSequenceFileCompressionType seqFileCompressionType, String lateRecordsLimit,
      LateRecordsAction lateRecordsAction, String lateRecordsDirPathTemplate,
      DataFormat dataFormat, CsvMode csvFileFormat, CsvHeader csvHeader, boolean csvReplaceNewLines, JsonMode jsonMode,
      String textFieldPath, boolean textEmptyLineIfNull) {
    this.hdfsUri = hdfsUri;
    this.hdfsKerberos = hdfsKerberos;
    this.kerberosPrincipal = kerberosPrincipal;
    this.kerberosKeytab = kerberosKeytab;
    this.hdfsConfigs = hdfsConfigs;
    this.uniquePrefix = uniquePrefix;
    this.dirPathTemplate = dirPathTemplate;
    this.timeZoneID = timeZoneID;
    this.timeDriver = timeDriver;
    this.maxRecordsPerFile = maxRecordsPerFile;
    this.maxFileSize = maxFileSize;
    this.compression = compression;
    this.fileType = fileType;
    this.keyEl = keyEl;
    this.seqFileCompressionType = seqFileCompressionType;
    this.lateRecordsLimit = lateRecordsLimit;
    this.lateRecordsAction = lateRecordsAction;
    this.lateRecordsDirPathTemplate = lateRecordsDirPathTemplate;
    this.dataFormat = dataFormat;
    this.csvFileFormat = csvFileFormat;
    this.csvHeader = csvHeader;
    this.csvReplaceNewLines = csvReplaceNewLines;
    this.jsonMode = jsonMode;
    this.textFieldPath = textFieldPath;
    this.textEmptyLineIfNull = textEmptyLineIfNull;
  }

  private Configuration hdfsConfiguration;
  private UserGroupInformation ugi;
  private long lateRecordsLimitSecs;
  private ActiveRecordWriters currentWriters;
  private ActiveRecordWriters lateWriters;
  private CharDataGeneratorFactory generatorFactory;
  private ELEval timeDriverElEval;
  private ELEval lateRecordsLimitEvaluator;
  private Date batchTime;

  @Override
  public List<ELEval> getElEvals(ElEvalProvider elEvalProvider) {
    return ImmutableList.of(
      ElUtil.createDirPathTemplateEval(elEvalProvider),
      ElUtil.createTimeDriverEval(elEvalProvider),
      ElUtil.createKeyElEval(elEvalProvider),
      ElUtil.createLateRecordsLimitEval(elEvalProvider),
      ElUtil.createLateRecordsDirPathTemplateEval(elEvalProvider)
    );
  }

  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues = super.validateConfigs();
    validateHadoopFS(issues);
    try {
      lateRecordsLimitEvaluator = ElUtil.createLateRecordsLimitEval(getContext());
      lateRecordsLimitSecs = lateRecordsLimitEvaluator.eval(getContext().getDefaultVariables(),
        lateRecordsLimit, Long.class);
      if (lateRecordsLimitSecs <= 0) {
        issues.add(getContext().createConfigIssue(Groups.LATE_RECORDS.name(), "lateRecordsLimit", Errors.HADOOPFS_10));
      }
    } catch (Exception ex) {
      issues.add(getContext().createConfigIssue(Groups.LATE_RECORDS.name(), "lateRecordsLimit", Errors.HADOOPFS_06,
                                                lateRecordsLimit, ex.getMessage(), ex));
    }
    if (maxFileSize < 0) {
      issues.add(getContext().createConfigIssue(Groups.LATE_RECORDS.name(), "maxFileSize", Errors.HADOOPFS_08));
    }

    if (maxRecordsPerFile < 0) {
      issues.add(getContext().createConfigIssue(Groups.LATE_RECORDS.name(), "maxRecordsPerFile", Errors.HADOOPFS_09));
    }

    if (uniquePrefix == null) {
      uniquePrefix = "";
    }

    validateDataFormat(issues);
    generatorFactory = createDataGeneratorFactory();

    SequenceFile.CompressionType compressionType = (seqFileCompressionType != null)
                                                   ? seqFileCompressionType.getType() : null;

    try {
      RecordWriterManager.validateDirPathTemplate1(getContext(), dirPathTemplate);
      RecordWriterManager.validateDirPathTemplate2(getContext(), dirPathTemplate);
      try {
        CompressionCodec compressionCodec = (CompressionMode.getCodec(compression) != null)
                                            ? CompressionMode.getCodec(compression).newInstance() : null;
        RecordWriterManager mgr = new RecordWriterManager(new URI(hdfsUri), hdfsConfiguration, uniquePrefix,
          dirPathTemplate, TimeZone.getTimeZone(timeZoneID), lateRecordsLimitSecs, maxFileSize, maxRecordsPerFile,
          fileType, compressionCodec, compressionType, keyEl, generatorFactory, getContext());

        currentWriters = new ActiveRecordWriters(mgr);
      } catch (Exception ex) {
        issues.add(getContext().createConfigIssue(Groups.OUTPUT_FILES.name(), null, Errors.HADOOPFS_11, ex.getMessage(),
                                                  ex));
      }
    } catch (Exception ex) {
      issues.add(getContext().createConfigIssue(Groups.OUTPUT_FILES.name(), "dirPathTemplate", Errors.HADOOPFS_20,
                                                ex.getMessage(), ex));
    }

    try {
      if (lateRecordsDirPathTemplate != null && !lateRecordsDirPathTemplate.isEmpty()) {
        RecordWriterManager.validateDirPathTemplate1(getContext(), lateRecordsDirPathTemplate);
        RecordWriterManager.validateDirPathTemplate2(getContext(), lateRecordsDirPathTemplate);
        try {
          CompressionCodec compressionCodec = (getCompressionCodec() != null)
                                              ? getCompressionCodec().newInstance() : null;
          RecordWriterManager mgr = new RecordWriterManager(new URI(hdfsUri), hdfsConfiguration, uniquePrefix,
            lateRecordsDirPathTemplate, TimeZone.getTimeZone(timeZoneID), lateRecordsLimitSecs, maxFileSize,
            maxRecordsPerFile, fileType, compressionCodec,compressionType, keyEl, generatorFactory, getContext());

          lateWriters = new ActiveRecordWriters(mgr);
        } catch (Exception ex) {
          issues.add(getContext().createConfigIssue(Groups.LATE_RECORDS.name(), null, Errors.HADOOPFS_17, ex.getMessage(), ex));
        }
      }
    } catch (Exception ex) {
      issues.add(getContext().createConfigIssue(Groups.OUTPUT_FILES.name(), "lateRecordsDirPathTemplate",
                                                Errors.HADOOPFS_21, ex.getMessage(), ex));
    }

    timeDriverElEval = ElUtil.createTimeDriverEval(getContext());
    try {
      ELEval.Variables variables = getContext().getDefaultVariables();
      RecordEl.setRecordInContext(variables, getContext().createRecord("validationConfigs"));
      TimeEl.setTimeNowInContext(variables, new Date());
      timeDriverElEval.eval(variables, timeDriver, Date.class);
    } catch (ELEvalException ex) {
      issues.add(getContext().createConfigIssue(Groups.OUTPUT_FILES.name(), "timeDriver", Errors.HADOOPFS_19,
                                                ex.getMessage(), ex));
    }

    return issues;
  }

  private void validateHadoopFS(List<ConfigIssue> issues) {
    try {
      boolean skipFS = false;
      hdfsConfiguration = new HdfsConfiguration();
      for (Map.Entry<String, String> config : hdfsConfigs.entrySet()) {
        hdfsConfiguration.set(config.getKey(), config.getValue());
      }
      if (!hdfsUri.contains("://")) {
        issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsUri", Errors.HADOOPFS_18, hdfsUri));
        skipFS = true;
      }
      hdfsConfiguration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, hdfsUri);
      if (hdfsKerberos) {
        hdfsConfiguration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
                              UserGroupInformation.AuthenticationMethod.KERBEROS.name());
        UserGroupInformation.setConfiguration(hdfsConfiguration);
        ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(kerberosPrincipal, kerberosKeytab);
        if (ugi.getAuthenticationMethod() != UserGroupInformation.AuthenticationMethod.KERBEROS) {
          issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsKerberos", Errors.HADOOPFS_00));
        }
      } else {
        hdfsConfiguration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
                              UserGroupInformation.AuthenticationMethod.SIMPLE.name());
        ugi = UserGroupInformation.getLoginUser();
      }
      if (!skipFS) {
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            try (FileSystem fs = getFileSystemForInitDestroy()) { //to trigger the close
            }
            return null;
          }
        });
      }
    } catch (Exception ex) {
      issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), null, Errors.HADOOPFS_01, hdfsUri,
                                                ex.getMessage(), ex));
    }
  }


  @Override
  protected void init() throws StageException {
    super.init();

    toHdfsRecordsCounter = getContext().createCounter("toHdfsRecords");
    toHdfsRecordsMeter = getContext().createMeter("toHdfsRecords");
    lateRecordsCounter = getContext().createCounter("lateRecords");
    lateRecordsMeter = getContext().createMeter("lateRecords");
  }

  private FileSystem getFileSystemForInitDestroy() throws IOException {
    try {
      return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return FileSystem.get(new URI(hdfsUri), hdfsConfiguration);
        }
      });
    } catch (IOException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  Configuration getHdfsConfiguration() {
    return hdfsConfiguration;
  }

  Class<? extends CompressionCodec> getCompressionCodec() throws StageException {
    return CompressionMode.getCodec(compression);
  }

  // for testing only
  long getLateRecordLimitSecs() {
    return lateRecordsLimitSecs;
  }

  private Map<String, String> getFieldPathToNameMapping(List<FieldPathToNameMappingConfig> mapping) {
    Map<String, String> fieldPathToNameMapping = new LinkedHashMap<>();
    if(mapping != null && !mapping.isEmpty()) {
      for (FieldPathToNameMappingConfig fieldPathToNameMappingConfig : mapping) {
        for(String field : fieldPathToNameMappingConfig.fields) {
          fieldPathToNameMapping.put(field, fieldPathToNameMappingConfig.name);
        }
      }
    }
    return fieldPathToNameMapping;
  }

  private void validateDataFormat(List<ConfigIssue> issues) {
    switch (dataFormat) {
      case TEXT:
      case JSON:
      case DELIMITED:
      case SDC_JSON:
        break;
      default:
        issues.add(getContext().createConfigIssue(Groups.OUTPUT_FILES.name(), "dataFormat", Errors.HADOOPFS_16,
                                                  dataFormat));
    }
  }

  private CharDataGeneratorFactory createDataGeneratorFactory() {
    CharDataGeneratorFactory.Builder builder = new CharDataGeneratorFactory.Builder(getContext(),
                                                                                    dataFormat.getGeneratorFormat());
    switch(dataFormat) {
      case JSON:
        builder.setMode(jsonMode);
        break;
      case DELIMITED:
        builder.setMode(csvFileFormat);
        builder.setMode(csvHeader);
        builder.setConfig(DelimitedCharDataGeneratorFactory.REPLACE_NEWLINES_KEY, csvReplaceNewLines);
        break;
      case TEXT:
        builder.setConfig(TextCharDataGeneratorFactory.FIELD_PATH_KEY, textFieldPath);
        builder.setConfig(TextCharDataGeneratorFactory.EMPTY_LINE_IF_NULL_KEY, textEmptyLineIfNull);
        break;
      case SDC_JSON:
        break;
      case XML:
      default:
        throw new IllegalStateException("It should not happen");
    }
    return builder.build();
  }

  @Override
  public void destroy() {
    try {
      if (currentWriters != null) {
        currentWriters.closeAll();
      }
      if (lateWriters != null) {
        lateWriters.closeAll();
      }
      if (ugi != null) {
        getFileSystemForInitDestroy().close();
      }
    } catch (IOException ex) {
      LOG.warn("Error while closing HDFS FileSystem URI='{}': {}", hdfsUri, ex.getMessage(), ex);
    }
    super.destroy();
  }

  @Override
  public void write(final Batch batch) throws StageException {
    setBatchTime();
    try {
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          getCurrentWriters().purge();
          if (getLateWriters() != null) {
            getLateWriters().purge();
          }
          HdfsTarget.super.write(batch);
          return null;
        }
      });
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  //visible for testing.
  Date setBatchTime() {
    batchTime = new Date();
    return batchTime;
  }

  protected Date getBatchTime() {
    return batchTime;
  }

  protected ActiveRecordWriters getCurrentWriters() {
    return currentWriters;
  }

  protected ActiveRecordWriters getLateWriters() {
    return lateWriters;
  }

  protected Date getRecordTime(Record record) throws ELEvalException {
    ELEval.Variables variables = getContext().getDefaultVariables();
    TimeEl.setTimeNowInContext(variables, getBatchTime());
    RecordEl.setRecordInContext(variables, record);
    return timeDriverElEval.eval(variables, timeDriver, Date.class);
  }

  private Counter toHdfsRecordsCounter;
  private Meter toHdfsRecordsMeter;
  private Counter lateRecordsCounter;
  private Meter lateRecordsMeter;

  @Override
  protected void write(Record record) throws StageException {
    try {
      Date recordTime = getRecordTime(record);
      RecordWriter writer = getCurrentWriters().get(getBatchTime(), recordTime, record);
      if (writer != null) {
        toHdfsRecordsCounter.inc();
        toHdfsRecordsMeter.mark();
        writer.write(record);
        getCurrentWriters().release(writer);
      } else {
        lateRecordsCounter.inc();
        lateRecordsMeter.mark();
        switch (lateRecordsAction) {
          case SEND_TO_ERROR:
            getContext().toError(record, Errors.HADOOPFS_12, record.getHeader().getSourceId());
            break;
          case SEND_TO_LATE_RECORDS_FILE:
            RecordWriter lateWriter = getLateWriters().get(getBatchTime(), getBatchTime(), record);
            lateWriter.write(record);
            getLateWriters().release(lateWriter);
            break;
          default:
            throw new RuntimeException("It should never happen");
        }
      }
    } catch (Exception ex) {
      throw new OnRecordErrorException(Errors.HADOOPFS_14, record, ex.getMessage(), ex);
    }
  }

}
