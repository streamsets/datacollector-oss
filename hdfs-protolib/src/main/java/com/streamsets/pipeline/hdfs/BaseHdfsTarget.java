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
import com.streamsets.pipeline.api.Label;
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

@ConfigGroups(BaseHdfsTarget.Groups.class)
public abstract class BaseHdfsTarget extends BaseTarget {
  private final static Logger LOG = LoggerFactory.getLogger(BaseHdfsTarget.class);

  public static enum Groups implements Label {
    HADOOP_FS("Hadoop FS"),
    OUTPUT_FILES("Output Files"),
    LATE_RECORDS("Late Records"),
    CSV("CSV Data"),
    TSV("Tab Separated Data"),
    ;

    private final String label;
    Groups(String label) {
      this.label = label;
    }

    @Override
    public String getLabel() {
      return label;
    }
  }

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Hadoop FS URI",
      description = "The URI of the Hadoop FileSystem",
      displayPosition = 10,
      group = "HADOOP_FS"
  )
  public String hdfsUri;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Kerberos Enabled",
      defaultValue = "false",
      description = "Indicates if Hadoop FileSystem requires Kerberos authentication",
      displayPosition = 20,
      group = "HADOOP_FS"
  )
  public boolean hdfsKerberos;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Kerberos principal",
      description = "Kerberos principal used to connect to Hadoop FileSystem",
      displayPosition = 30,
      group = "HADOOP_FS",
      dependsOn = "hdfsKerberos",
      triggeredByValue = "true"
  )
  public String kerberosPrincipal;

  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      label = "Kerberos keytab",
      description = "The location of the Kerberos keytab file with credentials for the Kerberos principal",
      displayPosition = 40,
      group = "HADOOP_FS",
      dependsOn = "hdfsKerberos",
      triggeredByValue = "true"
  )
  public String kerberosKeytab;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Hadoop FS configs",
      description = "Additional Hadoop FileSystem configuration to be used by the client (i.e. replication factor, " +
                    "rpc timeout, etc.)",
      displayPosition = 50,
      group = "HADOOP_FS"
  )
  public Map<String, String> hdfsConfigs;


  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Files Unique Prefix",
      description = "If using more than one Data Collector to write data to the same HDFS directories, each Data " +
                    "Collector must have a unique identifier. The identifier is used as file prefix for all the " +
                    "created by this Data Collector.",
      displayPosition = 105,
      group = "OUTPUT_FILES"
  )
  public String uniquePrefix;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.EL_STRING,
      defaultValue = "/tmp/out/${YYYY}-${MM}-${DD}-${hh}-${mm}-${ss}",
      label = "Directory Path Template",
      description = "Directory path template for the HDFS files, the template supports the following time tokens: " +
                    "${YY}, ${YYYY}, ${MM}, ${DD}, ${hh}, ${mm}, ${ss}, ${record:value(<field-path>)}. ",
      displayPosition = 110,
      group = "OUTPUT_FILES"
  )
  public String dirPathTemplate;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTC",
      label = "Timezone",
      description = "Timezone code used to resolve directory paths",
      displayPosition = 120,
      group = "OUTPUT_FILES"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = TimeZoneChooserValues.class)
  public String timeZoneID;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.EL_DATE,
      defaultValue = "${time:now()}",
      label = "Time Driver",
      description = "Date expression that indicates where the time should be taken for a record. It can be the " +
                    "'time:now()' or a 'record:value(<filepath>)' that resolves to a date",
      displayPosition = 130,
      group = "OUTPUT_FILES"
  )
  public String timeDriver;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.INTEGER,
      defaultValue = "0",
      label = "Max records per File",
      description = "Maximum number of records that trigger a file rollover. 0 does not trigger a rollover.",
      displayPosition = 140,
      group = "OUTPUT_FILES"
  )
  public long maxRecordsPerFile;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.INTEGER,
      defaultValue = "0",
      label = "Max file size (MBs)",
      description = "Maximum file size in Megabytes that trigger a file rollover. 0 does not trigger a rollover",
      displayPosition = 150,
      group = "OUTPUT_FILES"
  )
  public long maxFileSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NONE",
      label = "Compression Codec",
      description = "Compression codec to use",
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
      description = "Type of file type to create",
      displayPosition = 170,
      group = "OUTPUT_FILES"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = FileTypeChooserValues.class)
  public HdfsFileType fileType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "uuid()",
      label = "Sequence File Key",
      description = "Record key when using Hadoop Sequence Files. Valid values are a record:value('<field-path>') or " +
                    "uuid()",
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
      description = "Specifies the compression type for Sequence Files if using a CompressionCodec",
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
      label = "Late Records Time Limit (Secs)",
      description = "Time limit (in seconds) for a record to be written to the corresponding HDFS directory, if the " +
                    "limit is exceeded the record will be written to the current late records file. 0 means no limit. " +
                    "If a number is used it is considered seconds, it can be multiplied by 'MINUTES' or 'HOURS', ie: " +
                    "30 * MINUTES",
      displayPosition = 200,
      group = "LATE_RECORDS"
  )
  public String lateRecordsLimit;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "SEND_TO_ERROR",
      label = "Late Records",
      description = "Indicates what to do with late records",
      displayPosition = 210,
      group = "LATE_RECORDS"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = LateRecordsActionChooserValues.class)
  public HdfsLateRecordsAction lateRecordsAction;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.EL_STRING,
      defaultValue = "/tmp/late/${YYYY}-${MM}-${DD}",
      label = "Late Records Directory Path Template",
      description = "Directory path template for the HDFS files for late records, the template supports the following " +
                    "time tokens: ${YY}, ${YYYY}, ${MM}, ${DD}, ${hh}, ${mm}, ${ss}. IMPORTANT: Used only if " +
                    "'Late Records' is set to 'Send to late records file'.",
      displayPosition = 220,
      group = "LATE_RECORDS",
      dependsOn = "lateRecordsAction",
      triggeredByValue = "SEND_TO_LATE_RECORDS_FILE"
  )
  public String lateRecordsDirPathTemplate;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JSON",
      label = "Data Format",
      description = "Data Format",
      displayPosition = 100,
      group = "OUTPUT_FILES",
      dependsOn = "fileType",
      triggeredByValue = { "TEXT", "SEQUENCE_FILE"}
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = DataFormatChooserValues.class)
  public HdfsDataFormat dataFormat; //TODO


  /********  For CSV Content  ***********/

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "CSV",
      label = "CSV Format",
      description = "The specific CSV format of the files",
      displayPosition = 310,
      group = "CSV",
      dependsOn = "dataFormat",
      triggeredByValue = "CSV"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = CvsFileModeChooserValues.class)
  public CsvFileMode csvFileFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Field to Name Mapping",
      description = "Field to name mapping configuration",
      displayPosition = 320,
      group = "CSV",
      dependsOn = "dataFormat",
      triggeredByValue = "CSV"
  )
  @ComplexField
  public List<FieldPathToNameMappingConfig> cvsFieldPathToNameMappingConfigList;

  /********  For TSV Content  ***********/

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Field to Name Mapping",
      description = "Field to name mapping configuration",
      displayPosition = 320,
      group = "TSV",
      dependsOn = "dataFormat",
      triggeredByValue = "TSV"
  )
  @ComplexField
  public List<FieldPathToNameMappingConfig> tsvFieldPathToNameMappingConfigList;

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
      return ELEvaluator.evaluateHoursMinutesToSecondsExpr(lateRecordsLimit);
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
