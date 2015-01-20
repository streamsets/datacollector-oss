/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hdfs;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.hdfs.writer.ActiveRecordWriters;
import com.streamsets.pipeline.hdfs.writer.RecordWriterManager;
import com.streamsets.pipeline.lib.recordserialization.CsvRecordToString;
import com.streamsets.pipeline.lib.recordserialization.JsonRecordToString;
import com.streamsets.pipeline.lib.recordserialization.RecordToString;
import com.streamsets.pipeline.lib.recordserialization.XmlRecordToString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

@ConfigGroups(HdfsConfigGroups.class)
public abstract class BaseHdfsTarget extends BaseTarget {
  private final static Logger LOG = LoggerFactory.getLogger(BaseHdfsTarget.class);

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      description = "The URI of the Hadoop HDFS Cluster",
      label = "HDFS URI",
      displayPosition = 0)
  public String hdfsUri;

  @ConfigDef(required = true,
      type = ConfigDef.Type.BOOLEAN,
      description = "Indicates if HDFS requires Kerberos authentication",
      label = "Kerberos Enabled",
      defaultValue = "false",
      displayPosition = 1)

  public boolean hdfsKerberos;

  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      description = "Kerberos principal used to connect to HDFS",
      label = "Kerberos principal",
      dependsOn = "hdfsKerberos", triggeredByValue = "true",
      displayPosition = 2)
  public String kerberosPrincipal;

  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      description = "The location of the Kerberos keytab file with credentials for the Kerberos principal",
      label = "Kerberos keytab file",
      dependsOn = "hdfsKerberos", triggeredByValue = "true",
      displayPosition = 3)
  public String kerberosKeytab;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MAP,
      description = "Additional HDFS configuration to be used by the client (i.e. replication factor, rpc timeout, etc.)",
      label = "HDFS configs",
      displayPosition = 4)
  public Map<String, String> hdfsConfigs;


  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      description = "If using more than one Data Collector to write data to the same HDFS directories, each Data " +
                    "Collector must have a unique identifier. The identifier is used as file prefix for all the " +
                    "created by this Data Collector.",
      label = "Unique Identifier",
      group = "FILE",
      defaultValue = "",
      displayPosition = 99)
  public String uniquePrefix;

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      description = "Directory path template for the HDFS files, the template supports the following time tokens: " +
                    "${YY}, ${YYYY}, ${MM}, ${DD}, ${hh}, ${mm}, ${ss}, ${record:value(<field-path>)}. ",
      label = "Directory Path Template",
      group = "FILE",
      displayPosition = 100)
  public String dirPathTemplate;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Timezone code used to resolve directory paths",
      label = "Timezone",
      defaultValue = "UTC",
      group = "FILE",
      displayPosition = 101)
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = TimeZoneChooserValues.class)
  public String timeZoneID;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "File type to create in HDFS",
      label = "File Type",
      defaultValue = "TEXT",
      group = "FILE",
      displayPosition = 102)
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = FileTypeChooserValues.class)
  public HdfsFileType fileType;

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      description = "Record key when using Hadoop Sequence Files. Valid values are a record:value('<field-path>') or " +
                    "uuid()",
      label = "Sequence File Key",
      defaultValue = "uuid()",
      group = "FILE",
      displayPosition = 103,
      dependsOn = "fileType",
      triggeredByValue = "SEQUENCE_FILE")
  public String keyEl;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Compression codec to use",
      label = "Compression Codec",
      defaultValue = "NONE",
      group = "FILE",
      displayPosition = 104)
  @ValueChooser(type = ChooserMode.SUGGESTED, chooserValues = CompressionChooserValues.class)
  public String compression;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Specifies the compression type for Sequence Files if using a CompressionCodec",
      label = "Compression Type",
      defaultValue = "BLOCK",
      group = "FILE",
      displayPosition = 105,
      dependsOn = "fileType",
      triggeredByValue = "SEQUENCE_FILE")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = HdfsSequenceFileCompressionTypeChooserValues.class)
  public HdfsSequenceFileCompressionType seqFileCompressionType;

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      description = "Maximum number of records that trigger a file rollover. 0 does not trigger a rollover.",
      label = "Max records per File",
      defaultValue = "0",
      group = "FILE",
      displayPosition = 106)
  public long maxRecordsPerFile;

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      description = "Maximum file size in Megabytes that trigger a file rollover. 0 does not trigger a rollover",
      label = "Max file size (MBs)",
      defaultValue = "0",
      group = "FILE",
      displayPosition = 107)
  public long maxFileSize;

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      description = "Date expression that indicates where the time should be taken for a record. It can be the " +
                    "'time:now()' or a 'record:value(<filepath>)' that resolves to a date",
      label = "Time Driver",
      defaultValue = "time:now()",
      group = "FILE",
      displayPosition = 108)
  public String timeDriver;

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      description = "Time limit (in seconds) for a record to be written to the corresponding HDFS directory, if the " +
                    "limit is exceeded the record will be written to the current late records file. 0 means no limit. " +
                    "If a number is used it is considered seconds, it can be multiplied by 'MINUTES' or 'HOURS', ie: " +
                    "30 * MINUTES",
      label = "Late Records Time Limit (Secs)",
      defaultValue = "1 * HOURS",
      group = "LATE_RECORDS",
      displayPosition = 109)
  public String lateRecordsLimit;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Indicates what to do with late records",
      label = "Late Records",
      defaultValue = "SEND_TO_ERROR",
      group = "LATE_RECORDS",
      displayPosition = 110)
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = LateRecordsActionChooserValues.class)
  public HdfsLateRecordsAction lateRecordsAction;

  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      description = "Directory path template for the HDFS files for late records, the template supports the following " +
                    "time tokens: ${YY}, ${YYYY}, ${MM}, ${DD}, ${hh}, ${mm}, ${ss}. IMPORTANT: Used only if " +
                    "'Late Records' is set to 'Send to late records file'.",
      defaultValue = "",
      group = "LATE_RECORDS",
      label = "Late Records Directory Path Template",
      dependsOn = "lateRecordsAction",
      triggeredByValue = "SEND_TO_LATE_RECORDS_FILE",
      displayPosition = 111)
  public String lateRecordsDirPathTemplate;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Data Format",
      label = "Data Format",
      defaultValue = "JSON",
      group = "DATA",
      dependsOn = "fileType",
      triggeredByValue = { "TEXT", "SEQUENCE_FILE"},
      displayPosition = 200)
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = DataFormatChooserValues.class)
  public HdfsDataFormat dataFormat; //TODO



  /********  For CSV Content  ***********/

  @ConfigDef(required = false,
      type = ConfigDef.Type.MODEL,
      label = "CSV Format",
      description = "The specific CSV format of the files",
      defaultValue = "CSV",
      dependsOn = "dataFormat", triggeredByValue = {"CSV", "TSV"},
      group = "DATA",
      displayPosition = 201)
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = CvsFileModeChooserValues.class)
  public CsvFileMode csvFileFormat;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Field to name mapping configuration",
      label = "Field to Name Mapping",
      dependsOn = "dataFormat",
      triggeredByValue = {"CSV", "TSV"},
      defaultValue = "LOG",
      group = "DATA",
      displayPosition = 202)
  @ComplexField
  public List<FieldPathToNameMappingConfig> fieldPathToNameMappingConfigList;

  public static class FieldPathToNameMappingConfig {

    @ConfigDef(required = true,
        type = ConfigDef.Type.MODEL,
        label = "Field Path",
        description = "The fields which must be written to the target")
    @FieldSelector
    public List<String> fields;

    @ConfigDef(required = true,
        type = ConfigDef.Type.STRING,
        label = "Delimited Column Name",
        description = "The name which must be used for the fields in the target")
    public String name;
  }



  private static final String CONST_HOURS = "HOURS";
  private static final String CONST_MINUTES = "MINUTES";

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
      hdfsConfiguration.setBoolean("fs.hdfs.impl.disable.cache", false);
      if (hdfsKerberos) {
        hdfsConfiguration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
                              UserGroupInformation.AuthenticationMethod.KERBEROS.name());
        ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(kerberosPrincipal, kerberosKeytab);
        if (ugi.getAuthenticationMethod() != UserGroupInformation.AuthenticationMethod.KERBEROS) {
          throw new StageException(HdfsLibError.HDFS_0001);
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
      throw new StageException(HdfsLibError.HDFS_0002, hdfsUri, ex.getMessage(), ex);
    }

    long lateRecordLimitSecs = getLateRecordLimitSecs();
    if (lateRecordLimitSecs <= 0) {
      throw new StageException(HdfsLibError.HDFS_0013, lateRecordsLimit);
    }

    if (maxFileSize < 0) {
      throw new StageException(HdfsLibError.HDFS_0011, maxFileSize);
    }

    if (maxRecordsPerFile < 0) {
      throw new StageException(HdfsLibError.HDFS_0012, maxRecordsPerFile);
    }

    try {
      CompressionCodec compressionCodec = (CompressionMode.getCodec(compression) != null)
                                          ? CompressionMode.getCodec(compression).newInstance() : null;
      RecordWriterManager mgr = new RecordWriterManager(new URI(hdfsUri), hdfsConfiguration, uniquePrefix,
          dirPathTemplate, TimeZone.getTimeZone(timeZoneID), lateRecordLimitSecs, maxRecordsPerFile, maxFileSize,
          fileType, compressionCodec, seqFileCompressionType.getType(), keyEl, null);

      currentWriters = new ActiveRecordWriters(mgr);
    } catch (Exception ex) {

    }
    try {
      if (!lateRecordsDirPathTemplate.isEmpty()) {
        CompressionCodec compressionCodec = (getCompressionCodec() != null)
                                            ? getCompressionCodec().newInstance() : null;
        RecordWriterManager mgr = new RecordWriterManager(new URI(hdfsUri), hdfsConfiguration, uniquePrefix,
            lateRecordsDirPathTemplate, TimeZone.getTimeZone(timeZoneID), lateRecordLimitSecs, maxRecordsPerFile,
            maxFileSize, fileType, compressionCodec, seqFileCompressionType.getType(), keyEl, null);

        lateWriters = new ActiveRecordWriters(mgr);
      }
    } catch (Exception ex) {

    }

    timeDriverElEval = new ELEvaluator();
    ELRecordSupport.registerRecordFunctions(timeDriverElEval);
    timeDriverElEval.registerFunction("time", "now", TIME_NOW_FUNC);
    timeDriver = "${" + timeDriver + "}";

    recordToString = createRecordToStringInstance();
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
      ELEvaluator elEval = new ELEvaluator();
      elEval.registerConstant(CONST_MINUTES, 60);
      elEval.registerConstant(CONST_HOURS, 60 * 60);
      return (long) elEval.eval(new ELEvaluator.Variables(null, null), "${" + lateRecordsLimit + "}");
    } catch (Exception ex) {
      throw new StageException(HdfsLibError.HDFS_0008, lateRecordsLimit, ex.getMessage(), ex);
    }
  }

  static final String TIME_NOW_CONTEXT_VAR = "time_now";

  private static final Method TIME_NOW_FUNC;

  static {
    try {
      TIME_NOW_FUNC = BaseHdfsTarget.class.getMethod("getTimeNowFunc");
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

  private Map<String, String> getFieldPathToNameMapping() {
    Map<String, String> fieldPathToNameMapping = new LinkedHashMap<>();
    if(fieldPathToNameMappingConfigList != null && !fieldPathToNameMappingConfigList.isEmpty()) {
      for (FieldPathToNameMappingConfig fieldPathToNameMappingConfig : fieldPathToNameMappingConfigList) {
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
      case JSON:
        recordToString = new JsonRecordToString();
        break;
      case CSV:
        recordToString = new CsvRecordToString(csvFileFormat.getFormat());
        break;
      case XML:
        recordToString = new XmlRecordToString();
        break;
      default:
        throw new IllegalStateException(Utils.format("Invalid data format '{}'", dataFormat));
    }
    recordToString.setFieldPathToNameMapping(getFieldPathToNameMapping());
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
  public void write(Batch batch) throws StageException {
    setBatchTime();
    processBatch(batch);
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

  public abstract void processBatch(Batch batch) throws StageException;

}
