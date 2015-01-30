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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

@ConfigGroups(HdfsConfigGroups.class)
public abstract class BaseHdfsTarget extends BaseTarget {
  private final static Logger LOG = LoggerFactory.getLogger(BaseHdfsTarget.class);

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      description = "The URI of the Hadoop FileSystem",
      label = "Hadoop FS URI",
      displayPosition = 10,
      group = "HADOOP_FS")
  public String hdfsUri;

  @ConfigDef(required = true,
      type = ConfigDef.Type.BOOLEAN,
      description = "Indicates if Hadoop FileSystem requires Kerberos authentication",
      label = "Kerberos Enabled",
      defaultValue = "false",
      displayPosition = 20,
      group = "HADOOP_FS")
  public boolean hdfsKerberos;

  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      description = "Kerberos principal used to connect to Hadoop FileSystem",
      label = "Kerberos principal",
      dependsOn = "hdfsKerberos", triggeredByValue = "true",
      displayPosition = 30,
      group = "HADOOP_FS")
  public String kerberosPrincipal;

  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      description = "The location of the Kerberos keytab file with credentials for the Kerberos principal",
      label = "Kerberos keytab",
      dependsOn = "hdfsKerberos", triggeredByValue = "true",
      displayPosition = 40,
      group = "HADOOP_FS")
  public String kerberosKeytab;

  @ConfigDef(required = false,
      type = ConfigDef.Type.MAP,
      description = "Additional Hadoop FileSystem configuration to be used by the client (i.e. replication factor, " +
                    "rpc timeout, etc.)",
      label = "Hadoop FS configs",
      displayPosition = 50,
      group = "HADOOP_FS")
  public Map<String, String> hdfsConfigs;


  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      description = "If using more than one Data Collector to write data to the same HDFS directories, each Data " +
                    "Collector must have a unique identifier. The identifier is used as file prefix for all the " +
                    "created by this Data Collector.",
      label = "Files Unique Prefix",
      group = "OUTPUT_FILES",
      defaultValue = "",
      displayPosition = 105)
  public String uniquePrefix;

  @ConfigDef(required = true,
      type = ConfigDef.Type.EL_STRING,
      description = "Directory path template for the HDFS files, the template supports the following time tokens: " +
                    "${YY}, ${YYYY}, ${MM}, ${DD}, ${hh}, ${mm}, ${ss}, ${record:value(<field-path>)}. ",
      label = "Directory Path Template",
      defaultValue = "/tmp/out/${YYYY}-${MM}-${DD}-${hh}-${mm}-${ss}",
      group = "OUTPUT_FILES",
      displayPosition = 110)
  public String dirPathTemplate;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Timezone code used to resolve directory paths",
      label = "Timezone",
      defaultValue = "UTC",
      group = "OUTPUT_FILES",
      displayPosition = 120)
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = TimeZoneChooserValues.class)
  public String timeZoneID;

  @ConfigDef(required = true,
      type = ConfigDef.Type.EL_DATE,
      description = "Date expression that indicates where the time should be taken for a record. It can be the " +
                    "'time:now()' or a 'record:value(<filepath>)' that resolves to a date",
      label = "Time Driver",
      defaultValue = "${time:now()}",
      group = "OUTPUT_FILES",
      displayPosition = 130)
  public String timeDriver;

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      description = "Maximum number of records that trigger a file rollover. 0 does not trigger a rollover.",
      label = "Max records per File",
      defaultValue = "0",
      group = "OUTPUT_FILES",
      displayPosition = 140)
  public long maxRecordsPerFile;

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      description = "Maximum file size in Megabytes that trigger a file rollover. 0 does not trigger a rollover",
      label = "Max file size (MBs)",
      defaultValue = "0",
      group = "OUTPUT_FILES",
      displayPosition = 150)
  public long maxFileSize;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Compression codec to use",
      label = "Compression Codec",
      defaultValue = "NONE",
      group = "OUTPUT_FILES",
      displayPosition = 160)
  @ValueChooser(type = ChooserMode.SUGGESTED, chooserValues = CompressionChooserValues.class)
  public String compression;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Type of file type to create",
      label = "File Type",
      defaultValue = "TEXT",
      group = "OUTPUT_FILES",
      displayPosition = 170)
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = FileTypeChooserValues.class)
  public HdfsFileType fileType;

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      description = "Record key when using Hadoop Sequence Files. Valid values are a record:value('<field-path>') or " +
                    "uuid()",
      label = "Sequence File Key",
      defaultValue = "uuid()",
      group = "OUTPUT_FILES",
      displayPosition = 180,
      dependsOn = "fileType",
      triggeredByValue = "SEQUENCE_FILE")
  public String keyEl;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Specifies the compression type for Sequence Files if using a CompressionCodec",
      label = "Compression Type",
      defaultValue = "BLOCK",
      group = "OUTPUT_FILES",
      displayPosition = 190,
      dependsOn = "fileType",
      triggeredByValue = "SEQUENCE_FILE")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = HdfsSequenceFileCompressionTypeChooserValues.class)
  public HdfsSequenceFileCompressionType seqFileCompressionType;

  @ConfigDef(required = true,
      type = ConfigDef.Type.EL_NUMBER,
      description = "Time limit (in seconds) for a record to be written to the corresponding HDFS directory, if the " +
                    "limit is exceeded the record will be written to the current late records file. 0 means no limit. " +
                    "If a number is used it is considered seconds, it can be multiplied by 'MINUTES' or 'HOURS', ie: " +
                    "30 * MINUTES",
      label = "Late Records Time Limit (Secs)",
      defaultValue = "${1 * HOURS}",
      group = "LATE_RECORDS",
      displayPosition = 200)
  public String lateRecordsLimit;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Indicates what to do with late records",
      label = "Late Records",
      defaultValue = "SEND_TO_ERROR",
      group = "LATE_RECORDS",
      displayPosition = 210)
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = LateRecordsActionChooserValues.class)
  public HdfsLateRecordsAction lateRecordsAction;

  @ConfigDef(required = false,
      type = ConfigDef.Type.EL_STRING,
      description = "Directory path template for the HDFS files for late records, the template supports the following " +
                    "time tokens: ${YY}, ${YYYY}, ${MM}, ${DD}, ${hh}, ${mm}, ${ss}. IMPORTANT: Used only if " +
                    "'Late Records' is set to 'Send to late records file'.",
      defaultValue = "/tmp/late/${YYYY}-${MM}-${DD}",
      group = "LATE_RECORDS",
      label = "Late Records Directory Path Template",
      dependsOn = "lateRecordsAction",
      triggeredByValue = "SEND_TO_LATE_RECORDS_FILE",
      displayPosition = 220)
  public String lateRecordsDirPathTemplate;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Data Format",
      label = "Data Format",
      defaultValue = "JSON",
      group = "OUTPUT_FILES",
      dependsOn = "fileType",
      triggeredByValue = { "TEXT", "SEQUENCE_FILE"},
      displayPosition = 100)
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = DataFormatChooserValues.class)
  public HdfsDataFormat dataFormat; //TODO


  /********  For CSV Content  ***********/

  @ConfigDef(required = false,
      type = ConfigDef.Type.MODEL,
      label = "CSV Format",
      description = "The specific CSV format of the files",
      defaultValue = "CSV",
      dependsOn = "dataFormat", triggeredByValue = "CSV",
      group = "CSV",
      displayPosition = 310)
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = CvsFileModeChooserValues.class)
  public CsvFileMode csvFileFormat;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Field to name mapping configuration",
      label = "Field to Name Mapping",
      dependsOn = "dataFormat",
      triggeredByValue = "CSV",
      group = "CSV",
      displayPosition = 320)
  @ComplexField
  public List<FieldPathToNameMappingConfig> cvsFieldPathToNameMappingConfigList;

  /********  For TSV Content  ***********/

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Field to name mapping configuration",
      label = "Field to Name Mapping",
      dependsOn = "dataFormat",
      triggeredByValue = "TSV",
      group = "TSV",
      displayPosition = 320)
  @ComplexField
  public List<FieldPathToNameMappingConfig> tsvFieldPathToNameMappingConfigList;

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
      hdfsConfiguration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, hdfsUri);
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

    if (uniquePrefix == null) {
      uniquePrefix = "";
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
      throw new StageException(HdfsLibError.HDFS_0014, ex.getMessage(), ex);
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
      throw new StageException(HdfsLibError.HDFS_0014, ex.getMessage(), ex);
    }

    timeDriverElEval = new ELEvaluator();
    ELRecordSupport.registerRecordFunctions(timeDriverElEval);
    timeDriverElEval.registerFunction("time", "now", TIME_NOW_FUNC);
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
      return (long) elEval.eval(new ELEvaluator.Variables(null, null), lateRecordsLimit);
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
      case JSON:
        recordToString = new JsonRecordToString();
        break;
      case CSV:
        recordToString = new CsvRecordToString(csvFileFormat.getFormat());
        recordToString.setFieldPathToNameMapping(getFieldPathToNameMapping(cvsFieldPathToNameMappingConfigList));
        break;
      case TSV:
        recordToString = new CsvRecordToString(csvFileFormat.getFormat());
        recordToString.setFieldPathToNameMapping(getFieldPathToNameMapping(tsvFieldPathToNameMappingConfigList));
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

  public abstract void processBatch(Batch batch) throws StageException;

}
