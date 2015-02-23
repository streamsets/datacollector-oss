/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvModeChooserValues;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.TimeZoneChooserValues;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.stage.destination.hdfs.writer.ActiveRecordWriters;
import com.streamsets.pipeline.stage.destination.hdfs.writer.RecordWriter;
import com.streamsets.pipeline.stage.destination.hdfs.writer.RecordWriterManager;
import com.streamsets.pipeline.lib.recordserialization.CsvRecordToString;
import com.streamsets.pipeline.lib.recordserialization.DataCollectorRecordToString;
import com.streamsets.pipeline.lib.recordserialization.JsonRecordToString;
import com.streamsets.pipeline.lib.recordserialization.RecordToString;
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

@GenerateResourceBundle
@StageDef(
    version = "1.0.0",
    label = "Hadoop FS",
    description = "Writes to a Hadoop file system",
    icon = "hdfs.svg"
)
@ConfigGroups(Groups.class)
public class HdfsTarget extends BaseTarget {
  private final static Logger LOG = LoggerFactory.getLogger(HdfsTarget.class);

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Hadoop FS URI",
      description = "",
      displayPosition = 10,
      group = "HADOOP_FS"
  )
  public String hdfsUri;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Kerberos Authentication",
      defaultValue = "false",
      description = "",
      displayPosition = 20,
      group = "HADOOP_FS"
  )
  public boolean hdfsKerberos;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Kerberos principal",
      description = "",
      displayPosition = 30,
      group = "HADOOP_FS",
      dependsOn = "hdfsKerberos",
      triggeredByValue = "true"
  )
  public String kerberosPrincipal;

  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      label = "Kerberos keytab",
      description = "Keytab file path",
      displayPosition = 40,
      group = "HADOOP_FS",
      dependsOn = "hdfsKerberos",
      triggeredByValue = "true"
  )
  public String kerberosKeytab;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Hadoop FS Configuration",
      description = "Additional Hadoop properties to pass to the underlying Hadoop FileSystem",
      displayPosition = 50,
      group = "HADOOP_FS"
  )
  public Map<String, String> hdfsConfigs;


  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Files Prefix",
      description = "File name prefix",
      displayPosition = 105,
      group = "OUTPUT_FILES"
  )
  public String uniquePrefix;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.EL_STRING,
      defaultValue = "/tmp/out/${YYYY}-${MM}-${DD}-${hh}-${mm}-${ss}",
      label = "Directory Template",
      description = "Template for the creation of output directories. Valid variables are ${YYYY}, ${MM}, ${DD}, " +
                    "${hh}, ${mm}, ${ss} and {record:value(“/field”)} for values in a field. Directories are " +
                    "created based on the smallest time unit variable used.",
      displayPosition = 110,
      group = "OUTPUT_FILES"
  )
  public String dirPathTemplate;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTC",
      label = "Data Time Zone",
      description = "Time zone to use to resolve directory paths",
      displayPosition = 120,
      group = "OUTPUT_FILES"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = TimeZoneChooserValues.class)
  public String timeZoneID;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.EL_DATE,
      defaultValue = "${time:now()}",
      label = "Time Basis",
      description = "Time basis to use for a record. Enter an expression that evaluates to a datetime. To use the " +
                    "processing time, enter ${time:now()}. To use field values, use '${record:value(\"<filepath>\")}'.",
      displayPosition = 130,
      group = "OUTPUT_FILES"
  )
  public String timeDriver;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.INTEGER,
      defaultValue = "0",
      label = "Max records in a File",
      description = "Number of records that triggers the creation of a new file. Use 0 to opt out.",
      displayPosition = 140,
      group = "OUTPUT_FILES"
  )
  public long maxRecordsPerFile;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.INTEGER,
      defaultValue = "0",
      label = "Max file size (MB)",
      description = "Exceeding this size triggers the creation of a new file. Use 0 to opt out.",
      displayPosition = 150,
      group = "OUTPUT_FILES"
  )
  public long maxFileSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NONE",
      label = "Compression Codec",
      description = "",
      displayPosition = 160,
      group = "OUTPUT_FILES"
  )
  @ValueChooser(type = ChooserMode.SUGGESTED, chooserValues = CompressionChooserValues.class)
  public String compression;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TEXT",
      label = "File Type",
      description = "",
      displayPosition = 100,
      group = "OUTPUT_FILES"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = FileTypeChooserValues.class)
  public HdfsFileType fileType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "${uuid()}",
      label = "Sequence File Key",
      description = "Record key for creating Hadoop sequence files. Valid options are " +
                    "'${record:value(\"<field-path>\")}' or '${uuid()}'",
      displayPosition = 180,
      group = "OUTPUT_FILES",
      dependsOn = "fileType",
      triggeredByValue = "SEQUENCE_FILE"
  )
  public String keyEl;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "BLOCK",
      label = "Compression Type",
      description = "Compression type if using a CompressionCodec",
      displayPosition = 190,
      group = "OUTPUT_FILES",
      dependsOn = "fileType",
      triggeredByValue = "SEQUENCE_FILE"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = HdfsSequenceFileCompressionTypeChooserValues.class)
  public HdfsSequenceFileCompressionType seqFileCompressionType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.EL_NUMBER,
      defaultValue = "${1 * HOURS}",
      label = "Late Records Time Limit (secs)",
      description = "Time limit (in seconds) for a record to be written to the corresponding HDFS directory, if the " +
                    "limit is exceeded the record will be written to the current late records file. 0 means no limit. " +
                    "If a number is used it is considered seconds, it can be multiplied by 'MINUTES' or 'HOURS', ie: " +
                    "'${30 * MINUTES}'",
      displayPosition = 200,
      group = "LATE_RECORDS"
  )
  public String lateRecordsLimit;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TO_ERROR",
      label = "Late Record Handling",
      description = "Action for records considered late.",
      displayPosition = 210,
      group = "LATE_RECORDS"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = LateRecordsActionChooserValues.class)
  public LateRecordsAction lateRecordsAction;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.EL_STRING,
      defaultValue = "/tmp/late/${YYYY}-${MM}-${DD}",
      label = "Late Record Directory Template",
      description = "Template for the creation of late record directories. Valid variables are ${YYYY}, ${MM}, " +
                    "${DD}, ${hh}, ${mm}, ${ss}.",
      displayPosition = 220,
      group = "LATE_RECORDS",
      dependsOn = "lateRecordsAction",
      triggeredByValue = "SEND_TO_LATE_RECORDS_FILE"
  )
  public String lateRecordsDirPathTemplate;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "SDC_JSON",
      label = "Data Format",
      description = "Data Format",
      displayPosition = 100,
      group = "OUTPUT_FILES",
      dependsOn = "fileType",
      triggeredByValue = { "TEXT", "SEQUENCE_FILE"}
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = DataFormatChooserValues.class)
  public DataFormat dataFormat;


  /********  For DELIMITED Content  ***********/

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "SV",
      label = "CSV Format",
      description = "",
      displayPosition = 310,
      group = "DELIMITED",
      dependsOn = "dataFormat",
      triggeredByValue = "DELIMITED"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = CsvModeChooserValues.class)
  public CsvMode csvFileFormat;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Remove New Line Characters",
      description = "Replaces new lines characters with white spaces",
      displayPosition = 315,
      group = "DELIMITED",
      dependsOn = "dataFormat",
      triggeredByValue = "DELIMITED"
  )
  public boolean replaceNewLines;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Field Mapping",
      description = "",
      displayPosition = 320,
      group = "DELIMITED",
      dependsOn = "dataFormat",
      triggeredByValue = "DELIMITED"
  )
  @ComplexField
  public List<FieldPathToNameMappingConfig> cvsFieldPathToNameMappingConfigList;

  public static class FieldPathToNameMappingConfig {

    @ConfigDef(
        required = true,
        type = ConfigDef.Type.MODEL,
        label = "Field Path",
        description = "The fields which must be written to the target",
        displayPosition = 10
    )
    @FieldSelector
    public List<String> fields;

    @ConfigDef(required = true,
        type = ConfigDef.Type.STRING,
        label = "Delimited Column Name",
        description = "The name which must be used for the fields in the target",
        displayPosition = 20
    )
    public String name;
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
