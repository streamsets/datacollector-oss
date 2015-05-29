/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.RecordTarget;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.delimited.DelimitedDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.text.TextDataGeneratorFactory;
import com.streamsets.pipeline.stage.destination.hdfs.writer.ActiveRecordWriters;
import com.streamsets.pipeline.stage.destination.hdfs.writer.RecordWriter;
import com.streamsets.pipeline.stage.destination.hdfs.writer.RecordWriterManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class HdfsTarget extends RecordTarget {
  private final static Logger LOG = LoggerFactory.getLogger(HdfsTarget.class);
  private final static int MEGA_BYTE = 1024 * 1024;

  private final String hdfsUri;
  private final boolean hdfsKerberos;
  private final String kerberosPrincipal;
  private final String kerberosKeytab;
  private final String hadoopConfDir;
  private final Map<String, String> hdfsConfigs;
  private String uniquePrefix;
  private final String dirPathTemplate;
  private final String timeZoneID;
  private final String timeDriver;
  private final long maxRecordsPerFile;
  private final long maxFileSizeMBs;
  private final CompressionMode compression;
  private final String otherCompression;
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
  private String charset;

  public HdfsTarget(String hdfsUri, boolean hdfsKerberos, String kerberosPrincipal, String kerberosKeytab,
      String hadoopConfDir, Map<String, String> hdfsConfigs, String uniquePrefix, String dirPathTemplate,
      String timeZoneID,
      String timeDriver, long maxRecordsPerFile, long maxFileSize, CompressionMode compression, String otherCompression,
      HdfsFileType fileType, String keyEl,
      HdfsSequenceFileCompressionType seqFileCompressionType, String lateRecordsLimit,
      LateRecordsAction lateRecordsAction, String lateRecordsDirPathTemplate,
      DataFormat dataFormat, String charset, CsvMode csvFileFormat, CsvHeader csvHeader, boolean csvReplaceNewLines,
      JsonMode jsonMode,
      String textFieldPath, boolean textEmptyLineIfNull) {
    this.hdfsUri = hdfsUri;
    this.hdfsKerberos = hdfsKerberos;
    this.kerberosPrincipal = kerberosPrincipal;
    this.kerberosKeytab = kerberosKeytab;
    this.hadoopConfDir = hadoopConfDir;
    this.hdfsConfigs = hdfsConfigs;
    this.uniquePrefix = uniquePrefix;
    this.dirPathTemplate = dirPathTemplate;
    this.timeZoneID = timeZoneID;
    this.timeDriver = timeDriver;
    this.maxRecordsPerFile = maxRecordsPerFile;
    this.maxFileSizeMBs = maxFileSize;
    this.compression = compression;
    this.otherCompression = otherCompression;
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
    this.charset = charset;
  }

  private Configuration hdfsConfiguration;
  private UserGroupInformation ugi;
  private long lateRecordsLimitSecs;
  private ActiveRecordWriters currentWriters;
  private ActiveRecordWriters lateWriters;
  private DataGeneratorFactory generatorFactory;
  private ELEval timeDriverElEval;
  private ELEval lateRecordsLimitEvaluator;
  private Date batchTime;
  private CompressionCodec compressionCodec;

  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues = super.validateConfigs();
    boolean validHadoopFsUri = validateHadoopFS(issues);
    try {
      lateRecordsLimitEvaluator = getContext().createELEval("lateRecordsLimit");
      getContext().parseEL(lateRecordsLimit);
      lateRecordsLimitSecs = lateRecordsLimitEvaluator.eval(getContext().createELVars(),
        lateRecordsLimit, Long.class);
      if (lateRecordsLimitSecs <= 0) {
        issues.add(getContext().createConfigIssue(Groups.LATE_RECORDS.name(), "lateRecordsLimit", Errors.HADOOPFS_10));
      }
    } catch (Exception ex) {
      issues.add(getContext().createConfigIssue(Groups.LATE_RECORDS.name(), "lateRecordsLimit", Errors.HADOOPFS_06,
                                                lateRecordsLimit, ex.getMessage(), ex));
    }
    if (maxFileSizeMBs < 0) {
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
        switch (compression) {
          case OTHER:
            try {
              Class klass = Thread.currentThread().getContextClassLoader().loadClass(otherCompression);
              if (CompressionCodec.class.isAssignableFrom(klass)) {
                compressionCodec = ((Class<? extends CompressionCodec> ) klass).newInstance();
              } else {
                throw new StageException(Errors.HADOOPFS_04, otherCompression);
              }
            } catch (Exception ex1) {
              throw new StageException(Errors.HADOOPFS_05, otherCompression, ex1.getMessage(), ex1);
            }
            break;
          case NONE:
            break;
          default:
            compressionCodec = compression.getCodec().newInstance();
            break;
        }
        if(validHadoopFsUri) {
          RecordWriterManager mgr = new RecordWriterManager(new URI(hdfsUri), hdfsConfiguration, uniquePrefix,
            dirPathTemplate, TimeZone.getTimeZone(timeZoneID), lateRecordsLimitSecs, maxFileSizeMBs * MEGA_BYTE,
            maxRecordsPerFile, fileType, compressionCodec, compressionType, keyEl, generatorFactory, getContext());

          currentWriters = new ActiveRecordWriters(mgr);
        }
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
        if(validHadoopFsUri) {
          try {
            RecordWriterManager mgr = new RecordWriterManager(new URI(hdfsUri), hdfsConfiguration, uniquePrefix,
              lateRecordsDirPathTemplate, TimeZone.getTimeZone(timeZoneID), lateRecordsLimitSecs,
              maxFileSizeMBs * MEGA_BYTE, maxRecordsPerFile, fileType, compressionCodec, compressionType, keyEl,
              generatorFactory, getContext());

            lateWriters = new ActiveRecordWriters(mgr);
          } catch (Exception ex) {
            issues.add(getContext().createConfigIssue(Groups.LATE_RECORDS.name(), null, Errors.HADOOPFS_17, ex.getMessage(), ex));
          }
        }
      }
    } catch (Exception ex) {
      issues.add(getContext().createConfigIssue(Groups.OUTPUT_FILES.name(), "lateRecordsDirPathTemplate",
                                                Errors.HADOOPFS_21, ex.getMessage(), ex));
    }

    timeDriverElEval = getContext().createELEval("timeDriver");
    try {
      ELVars variables = getContext().createELVars();
      RecordEL.setRecordInContext(variables, getContext().createRecord("validationConfigs"));
      TimeEL.setTimeNowInContext(variables, new Date());
      getContext().parseEL(timeDriver);
      timeDriverElEval.eval(variables, timeDriver, Date.class);
    } catch (ELEvalException ex) {
      issues.add(getContext().createConfigIssue(Groups.OUTPUT_FILES.name(), "timeDriver", Errors.HADOOPFS_19,
                                                ex.getMessage(), ex));
    }

    return issues;
  }

  Configuration getHadoopConfiguration(List<ConfigIssue> issues) {
    Configuration conf = new Configuration();

    if (hadoopConfDir != null && !hadoopConfDir.isEmpty()) {
      File hadoopConfigDir = new File(hadoopConfDir);
      if (!hadoopConfigDir.isAbsolute()) {
        hadoopConfigDir = new File(getContext().getResourcesDirectory(), hadoopConfDir).getAbsoluteFile();
      }
      if (!hadoopConfigDir.exists()) {
        issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hadoopConfDir", Errors.HADOOPFS_25,
                                                  hadoopConfDir));
      } else if (!hadoopConfigDir.isDirectory()) {
        issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hadoopConfDir", Errors.HADOOPFS_26,
                                                  hadoopConfDir));
      } else {
        File coreSite = new File(hadoopConfigDir, "core-site.xml");
        if (coreSite.exists()) {
          if (!coreSite.isFile()) {
            issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hadoopConfDir", Errors.HADOOPFS_27,
                                                      hadoopConfDir, "core-site.xml"));
          }
          conf.addResource(new Path(coreSite.getAbsolutePath()));
        }
        File hdfsSite = new File(hadoopConfigDir, "hdfs-site.xml");
        if (hdfsSite.exists()) {
          if (!hdfsSite.isFile()) {
            issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hadoopConfDir", Errors.HADOOPFS_27,
                                                      hadoopConfDir, "hdfs-site.xml"));
          }
          conf.addResource(new Path(hdfsSite.getAbsolutePath()));
        }
      }
    }
    for (Map.Entry<String, String> config : hdfsConfigs.entrySet()) {
      conf.set(config.getKey(), config.getValue());
    }
    return conf;
  }

  private boolean validateHadoopFS(List<ConfigIssue> issues) {
    boolean validHapoopFsUri = true;
    if (hdfsUri.contains("://")) {
      try {
        new URI(hdfsUri);
      } catch (Exception ex) {
        issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), null, Errors.HADOOPFS_22, hdfsUri,
          ex.getMessage(), ex));
        validHapoopFsUri = false;
      }
    } else {
      issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsUri", Errors.HADOOPFS_18, hdfsUri));
      validHapoopFsUri = false;
    }

    StringBuilder logMessage = new StringBuilder();
    try {
      hdfsConfiguration = getHadoopConfiguration(issues);

      hdfsConfiguration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, hdfsUri);
      if (hdfsKerberos) {
        logMessage.append("Using Kerberos: ");
        if (Boolean.getBoolean("sdc.transient-env")) {
          Utils.checkState(getContext().isClusterMode(), "Transient environment implies cluster mode");
          Utils.checkState(!getContext().isPreview(), "Transient environment implies pipeline is not in preview");
          logMessage.append("via delegation token: " + System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
          // use delegation token
          UserGroupInformation.setConfiguration(hdfsConfiguration);
          ugi = UserGroupInformation.getCurrentUser();
        } else {
          logMessage.append("via keytab");
          hdfsConfiguration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
            UserGroupInformation.AuthenticationMethod.KERBEROS.name());
          UserGroupInformation.setConfiguration(hdfsConfiguration);
          ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(kerberosPrincipal, kerberosKeytab);
          if (ugi.getAuthenticationMethod() != UserGroupInformation.AuthenticationMethod.KERBEROS) {
            issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsKerberos", Errors.HADOOPFS_00,
              ugi.getAuthenticationMethod(), UserGroupInformation.AuthenticationMethod.KERBEROS));
          }
        }
      } else {
        logMessage.append("Using Simple");
        hdfsConfiguration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
                              UserGroupInformation.AuthenticationMethod.SIMPLE.name());
        ugi = UserGroupInformation.getLoginUser();
      }
      if (validHapoopFsUri) {
        // we just login, the TGT won't expire yet, no need to relogin
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
    LOG.info("Authentication Config: " + logMessage);
    return validHapoopFsUri;
  }


  @Override
  protected void init() throws StageException {
    super.init();
    try {
      FileSystem fs = getFileSystemForInitDestroy();
      getCurrentWriters().commitOldFiles(fs);
      if (getLateWriters() != null) {
        getLateWriters().commitOldFiles(fs);
      }
    } catch (IOException ex) {
      throw new StageException(Errors.HADOOPFS_23, ex.getMessage(), ex);
    }
    toHdfsRecordsCounter = getContext().createCounter("toHdfsRecords");
    toHdfsRecordsMeter = getContext().createMeter("toHdfsRecords");
    lateRecordsCounter = getContext().createCounter("lateRecords");
    lateRecordsMeter = getContext().createMeter("lateRecords");
  }

  private FileSystem getFileSystemForInitDestroy() throws IOException {
    try {
      // we need to relogin if the TGT is expiring
      ugi.checkTGTAndReloginFromKeytab();
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

  CompressionCodec getCompressionCodec() throws StageException {
    return compressionCodec;
  }

  // for testing only
  long getLateRecordLimitSecs() {
    return lateRecordsLimitSecs;
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

  private DataGeneratorFactory createDataGeneratorFactory() {
    DataGeneratorFactoryBuilder builder = new DataGeneratorFactoryBuilder(getContext(),
      dataFormat.getGeneratorFormat());
    if(charset == null || charset.trim().isEmpty()) {
      charset = "UTF-8";
    }
    builder.setCharset(Charset.forName(charset));
    switch(dataFormat) {
      case JSON:
        builder.setMode(jsonMode);
        break;
      case DELIMITED:
        builder.setMode(csvFileFormat);
        builder.setMode(csvHeader);
        builder.setConfig(DelimitedDataGeneratorFactory.REPLACE_NEWLINES_KEY, csvReplaceNewLines);
        break;
      case TEXT:
        builder.setConfig(TextDataGeneratorFactory.FIELD_PATH_KEY, textFieldPath);
        builder.setConfig(TextDataGeneratorFactory.EMPTY_LINE_IF_NULL_KEY, textEmptyLineIfNull);
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
      // we need to relogin if the TGT is expiring
      ugi.checkTGTAndReloginFromKeytab();
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
    ELVars variables = getContext().createELVars();
    TimeEL.setTimeNowInContext(variables, getBatchTime());
    RecordEL.setRecordInContext(variables, record);
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
    } catch (IOException ex) {
      throw new StageException(Errors.HADOOPFS_14, record, ex.getMessage(), ex);
    } catch (StageException ex) {
      throw new OnRecordErrorException(Errors.HADOOPFS_14, record, ex.getMessage(), ex);
    }
  }

}
