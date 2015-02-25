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
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.lib.recordserialization.CsvRecordToString;
import com.streamsets.pipeline.lib.recordserialization.DataCollectorRecordToString;
import com.streamsets.pipeline.lib.recordserialization.JsonRecordToString;
import com.streamsets.pipeline.lib.recordserialization.RecordToString;
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

import javax.servlet.jsp.el.ELException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class HdfsTarget extends BaseTarget {
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
  private final boolean replaceNewLines;
  private final List<FieldPathToNameMappingConfig> cvsFieldPathToNameMappingConfigList;


  public HdfsTarget(String hdfsUri, boolean hdfsKerberos, String kerberosPrincipal, String kerberosKeytab,
      Map<String, String> hdfsConfigs, String uniquePrefix, String dirPathTemplate, String timeZoneID,
      String timeDriver, long maxRecordsPerFile, long maxFileSize, String compression,
      HdfsFileType fileType, String keyEl,
      HdfsSequenceFileCompressionType seqFileCompressionType, String lateRecordsLimit,
      LateRecordsAction lateRecordsAction, String lateRecordsDirPathTemplate,
      DataFormat dataFormat, CsvMode csvFileFormat, boolean replaceNewLines,
      List<FieldPathToNameMappingConfig> cvsFieldPathToNameMappingConfigList) {
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
    this.replaceNewLines = replaceNewLines;
    this.cvsFieldPathToNameMappingConfigList = cvsFieldPathToNameMappingConfigList;
  }

  private Configuration hdfsConfiguration;
  private UserGroupInformation ugi;
  private ELEvaluator timeDriverElEval;
  private ActiveRecordWriters currentWriters;
  private ActiveRecordWriters lateWriters;
  private RecordToString recordToString;

  private Date batchTime;

  @Override
  protected void init() throws StageException {
    super.init();
    try {
      hdfsConfiguration = new HdfsConfiguration();
      for (Map.Entry<String, String> config : hdfsConfigs.entrySet()) {
        hdfsConfiguration.set(config.getKey(), config.getValue());
      }
      hdfsConfiguration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, hdfsUri);
      if (hdfsKerberos) {
        hdfsConfiguration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
                              UserGroupInformation.AuthenticationMethod.KERBEROS.name());
        ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(kerberosPrincipal, kerberosKeytab);
        if (ugi.getAuthenticationMethod() != UserGroupInformation.AuthenticationMethod.KERBEROS) {
          throw new StageException(Errors.HADOOPFS_00);
        }
      } else {
        hdfsConfiguration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
                              UserGroupInformation.AuthenticationMethod.SIMPLE.name());
        ugi = UserGroupInformation.getLoginUser();
      }
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          getFileSystemForInitDestroy();
          return null;
        }
      });
    } catch (Exception ex) {
      throw new StageException(Errors.HADOOPFS_01, hdfsUri, ex.getMessage(), ex);
    }

    if (uniquePrefix == null) {
      uniquePrefix = "";
    }

    long lateRecordLimitSecs = getLateRecordLimitSecs();
    if (lateRecordLimitSecs <= 0) {
      throw new StageException(Errors.HADOOPFS_10, lateRecordsLimit);
    }

    if (maxFileSize < 0) {
      throw new StageException(Errors.HADOOPFS_08, maxFileSize);
    }

    if (maxRecordsPerFile < 0) {
      throw new StageException(Errors.HADOOPFS_09, maxRecordsPerFile);
    }

    recordToString = createRecordToStringInstance();

    SequenceFile.CompressionType compressionType = (seqFileCompressionType != null)
                                                   ? seqFileCompressionType.getType() : null;
    try {
      CompressionCodec compressionCodec = (CompressionMode.getCodec(compression) != null)
                                          ? CompressionMode.getCodec(compression).newInstance() : null;
      RecordWriterManager mgr = new RecordWriterManager(new URI(hdfsUri), hdfsConfiguration, uniquePrefix,
                                                        dirPathTemplate, TimeZone.getTimeZone(timeZoneID), lateRecordLimitSecs, maxFileSize, maxRecordsPerFile,
                                                        fileType, compressionCodec, compressionType, keyEl, recordToString);

      currentWriters = new ActiveRecordWriters(mgr);
    } catch (Exception ex) {
      throw new StageException(Errors.HADOOPFS_11, ex.getMessage(), ex);
    }

    try {
      if (lateRecordsDirPathTemplate != null && !lateRecordsDirPathTemplate.isEmpty()) {
        CompressionCodec compressionCodec = (getCompressionCodec() != null)
                                            ? getCompressionCodec().newInstance() : null;
        RecordWriterManager mgr = new RecordWriterManager(new URI(hdfsUri), hdfsConfiguration, uniquePrefix,
                                                          lateRecordsDirPathTemplate, TimeZone.getTimeZone(timeZoneID), lateRecordLimitSecs, maxFileSize,
                                                          maxRecordsPerFile, fileType, compressionCodec, compressionType, keyEl, recordToString);

        lateWriters = new ActiveRecordWriters(mgr);
      }
    } catch (Exception ex) {
      throw new StageException(Errors.HADOOPFS_11, ex.getMessage(), ex);
    }

    timeDriverElEval = new ELEvaluator();
    ELRecordSupport.registerRecordFunctions(timeDriverElEval);
    timeDriverElEval.registerFunction("time", "now", TIME_NOW_FUNC);

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

  long getLateRecordLimitSecs() throws StageException {
    try {
      return ELEvaluator.evaluateHoursMinutesToSecondsExpr(lateRecordsLimit);
    } catch (Exception ex) {
      throw new StageException(Errors.HADOOPFS_06, lateRecordsLimit, ex.getMessage(), ex);
    }
  }

  static final String TIME_NOW_CONTEXT_VAR = "time_now";

  private static final Method TIME_NOW_FUNC;

  static {
    try {
      TIME_NOW_FUNC = HdfsTarget.class.getMethod("getTimeNowFunc");
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static Date getTimeNowFunc() {
    Date now = (Date) ELEvaluator.getVariablesInScope().getContextVariable(TIME_NOW_CONTEXT_VAR);
    Utils.checkArgument(now != null, "time:now() function has not been properly initialized");
    return now;
  }

  static void setTimeNowInContext(ELEvaluator.Variables variables, Date now) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(TIME_NOW_CONTEXT_VAR, now);
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

  private RecordToString createRecordToStringInstance() {
    RecordToString recordToString;
    switch(dataFormat) {
      case SDC_JSON:
        recordToString = new DataCollectorRecordToString(getContext());
        break;
      case JSON:
        recordToString = new JsonRecordToString();
        break;
      case DELIMITED:
        recordToString = new CsvRecordToString(csvFileFormat.getFormat(), replaceNewLines);
        recordToString.setFieldPathToNameMapping(getFieldPathToNameMapping(cvsFieldPathToNameMappingConfigList));
        break;
      default:
        throw new IllegalStateException(Utils.format("Invalid data format '{}'", dataFormat));
    }
    return recordToString;
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
      getFileSystemForInitDestroy().close();
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
          processBatch(batch);
          return null;
        }
      });
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void setBatchTime() {
    batchTime = new Date();
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

  protected Date getRecordTime(Record record) throws ELException {
    ELEvaluator.Variables variables = new ELEvaluator.Variables(null, null);
    setTimeNowInContext(variables, getBatchTime());
    ELRecordSupport.setRecordInContext(variables, record);
    return (Date) timeDriverElEval.eval(variables, timeDriver);
  }

  private Counter toHdfsRecordsCounter;
  private Meter toHdfsRecordsMeter;
  private Counter lateRecordsCounter;
  private Meter lateRecordsMeter;


  public void processBatch(Batch batch) throws StageException {
    try {
      getCurrentWriters().purge();
      if (getLateWriters() != null) {
        getLateWriters().purge();
      }
      Iterator<Record> it = batch.getRecords();
      while (it.hasNext()) {
        processRecord(it.next());
      }
    } catch (IOException ex) {
      throw new StageException(Errors.HADOOPFS_13, ex.getMessage(), ex);
    }
  }

  protected void processRecord(Record record) throws StageException {
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
      switch (getContext().getOnErrorRecord()) {
        case DISCARD:
          break;
        case TO_ERROR:
          getContext().toError(record, ex);
          break;
        case STOP_PIPELINE:
          throw new StageException(Errors.HADOOPFS_14, record, ex.getMessage(), ex);
        default:
          throw new RuntimeException("It should never happen");
      }
    }
  }

}
