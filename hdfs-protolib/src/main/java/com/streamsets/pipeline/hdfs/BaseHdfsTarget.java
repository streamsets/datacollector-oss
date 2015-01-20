/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hdfs;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import org.apache.hadoop.conf.Configuration;
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
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

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

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      description = "Kerberos principal used to connect to HDFS",
      label = "Kerberos principal",
      dependsOn = "hdfsKerberos", triggeredByValue = "true",
      displayPosition = 2)
  public String kerberosPrincipal;

  @ConfigDef(required = true,
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


  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      description = "Directory path template for the HDFS files, the template supports the following time tokens: " +
                    "${YY}, ${YYYY}, ${MM}, ${DD}, ${hh}, ${mm}, ${ss}, ${record:value(<fieldpath>)}. ",
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
      description = "Record key when using Hadoop Sequence Files. Valid values are a record:value('<fieldpath>') or " +
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
      type = ConfigDef.Type.INTEGER,
      description = "Maximum number of records that trigger a file rollover. 0 does not trigger a rollover.",
      label = "Max records per File",
      defaultValue = "0",
      group = "FILE",
      displayPosition = 105)
  public long maxRecordsPerFile;

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      description = "Maximum file size in Megabytes that trigger a file rollover. 0 does not trigger a rollover",
      label = "Max file size (MBs)",
      defaultValue = "0",
      group = "FILE",
      displayPosition = 106)
  public long maxFileSize;

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      description = "Date expression that indicates where the time should be taken for a record. It can be the " +
                    "'time:now()' or a 'record:value(<filepath>)' that resolves to a date",
      label = "Time Driver",
      defaultValue = "time:now()",
      group = "FILE",
      displayPosition = 107)
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
      displayPosition = 108)
  public String lateRecordsLimit;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Indicates if the time should be driven by the Data Collector clock or by a time specified in " +
                    "a record field",
      label = "Late Records",
      defaultValue = "SEND_TO_ERROR",
      group = "LATE_RECORDS",
      displayPosition = 109)
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
      displayPosition = 110)
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
  public HdfsDataFormat dataFormat;

  private static final String CONST_YYYY = "YYYY";
  private static final String CONST_YY = "YY";
  private static final String CONST_MM = "MM";
  private static final String CONST_DD = "DD";
  private static final String CONST_hh = "hh";
  private static final String CONST_mm = "mm";
  private static final String CONST_ss = "ss";

  private static final String CONST_HOURS = "HOURS";
  private static final String CONST_MINUTES = "MINUTES";

  private Configuration hdfsConfiguration;
  private UserGroupInformation ugi;
  private TimeZone timeZone;
  private ELEvaluator pathElEval;
  private ELEvaluator timeDriverElEval;
  private ELEvaluator keyElEval;
  private Class<? extends CompressionCodec> compressionCodec;
  private long lateRecordLimitSecs;
  private Date batchTime;

  Map<String, Object> getELVarsForTime(Date date) {
    Calendar calendar = Calendar.getInstance(timeZone);
    calendar.setTime(date);
    Map<String, Object> map = new HashMap<>();
    String year = String.format("%04d", calendar.get(Calendar.YEAR));
    map.put(CONST_YYYY, year);
    map.put(CONST_YY, year.substring(year.length() - 2));
    map.put(CONST_MM, String.format("%02d", calendar.get(Calendar.MONTH) + 1));
    map.put(CONST_DD, String.format("%02d", calendar.get(Calendar.DAY_OF_MONTH)));
    map.put(CONST_hh, String.format("%02d", calendar.get(Calendar.HOUR_OF_DAY)));
    map.put(CONST_mm, String.format("%02d", calendar.get(Calendar.MINUTE)));
    map.put(CONST_ss, String.format("%02d", calendar.get(Calendar.SECOND)));
    return map;
  }

  @Override
  protected void init() throws StageException {
    super.init();
    setBatchTime(); // to validate ELs
    try {
      hdfsConfiguration = new HdfsConfiguration();
      for (Map.Entry<String, String> config : hdfsConfigs.entrySet()) {
        hdfsConfiguration.set(config.getKey(), config.getValue());
      }
      //TODO: configure FS caching
      if (hdfsKerberos) {
        ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(kerberosPrincipal, kerberosKeytab);
        if (ugi.getAuthenticationMethod() != UserGroupInformation.AuthenticationMethod.KERBEROS) {
          throw new StageException(HdfsLibError.HDFS_0001);
        }
      } else {
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

    timeZone = TimeZone.getTimeZone(timeZoneID);

    pathElEval = new ELEvaluator();
    ELRecordSupport.registerRecordFunctions(pathElEval);

    validateDirPathTemplate(dirPathTemplate, HdfsLibError.HDFS_0003);

    if (!lateRecordsDirPathTemplate.isEmpty()) {
      validateDirPathTemplate(lateRecordsDirPathTemplate, HdfsLibError.HDFS_0004);
    }
    compressionCodec = CompressionMode.getCodec(compression);

    try {
      ELEvaluator elEval = new ELEvaluator();
      elEval.registerConstant(CONST_MINUTES, 60);
      elEval.registerConstant(CONST_HOURS, 60 * 60);
      lateRecordsLimit = "${" + lateRecordsLimit + "}";
      lateRecordLimitSecs = (long) elEval.eval(new ELEvaluator.Variables(null, null), lateRecordsLimit);
    } catch (Exception ex) {
      throw new StageException(HdfsLibError.HDFS_0008, lateRecordsLimit, ex.getMessage(), ex);
    }
    if (lateRecordLimitSecs <= 0) {
      throw new StageException(HdfsLibError.HDFS_0013, lateRecordsLimit);
    }

    timeDriverElEval = new ELEvaluator();
    ELRecordSupport.registerRecordFunctions(timeDriverElEval);
    timeDriverElEval.registerFunction("time", "now", TIME_NOW_FUNC);

    timeDriver = "${" + timeDriver + "}";
    validateTimeDriver(timeDriver);

    keyEl = "${" + keyEl + "}";
    keyElEval = new ELEvaluator();
    ELRecordSupport.registerRecordFunctions(keyElEval);
    keyElEval.registerFunction("", "uuid", UUID_FUNC);

    try {
      ELEvaluator.Variables vars = new ELEvaluator.Variables(null, null);
      keyElEval.eval(vars, keyEl);
    } catch (ELException ex) {
      throw new StageException(HdfsLibError.HDFS_0009, keyEl, ex.getMessage(), ex);
    }

    if (maxFileSize < 0) {
      throw new StageException(HdfsLibError.HDFS_0011, maxFileSize);
    }

    if (maxRecordsPerFile < 0) {
      throw new StageException(HdfsLibError.HDFS_0012, maxRecordsPerFile);
    }
  }

  protected Configuration getHdfsConfiguration() {
    return hdfsConfiguration;
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
  static final String TIME_NOW_CONTEXT_VAR = "time_now";

  private static final Method TIME_NOW_FUNC;
  private static final Method UUID_FUNC;

  static {
    try {
      TIME_NOW_FUNC = BaseHdfsTarget.class.getMethod("getTimeNowFunc");
      UUID_FUNC = BaseHdfsTarget.class.getMethod("getUUIDFunc");
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static Date getTimeNowFunc() {
    Date now = (Date) ELEvaluator.getVariablesInScope().getContextVariable(TIME_NOW_CONTEXT_VAR);
    Utils.checkArgument(now != null, "time:now() function has not been properly initialized");
    return now;
  }

  public static String getUUIDFunc() {
    return UUID.randomUUID().toString();
  }

  public static void setTimeNowInContext(ELEvaluator.Variables variables, Date now) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(TIME_NOW_CONTEXT_VAR, now);
  }

  private void validateDirPathTemplate(String template, HdfsLibError error) throws StageException {
    try {
      ELEvaluator.Variables vars = new ELEvaluator.Variables(getELVarsForTime(new Date()), null);
      pathElEval.eval(vars, template);
    } catch (ELException ex) {
      throw new StageException(error, template, ex.getMessage(), ex);
    }
  }

  private void validateTimeDriver(String template) throws StageException {
    try {
      getRecordTime(null);
    } catch (ELException ex) {
      throw new StageException(HdfsLibError.HDFS_0009, template, ex.getMessage(), ex);
    }
  }

  protected TimeZone getTimeZone() {
    return timeZone;
  }

  protected ELEvaluator getPathElEvaluator() {
    return pathElEval;
  }

  protected long getLateRecordLimitSecs() {
    return lateRecordLimitSecs;
  }

  protected Date getRecordTime(Record record) throws ELException {
    ELEvaluator.Variables variables = new ELEvaluator.Variables(null, null);
    setTimeNowInContext(variables, getBatchTime());
    ELRecordSupport.setRecordInContext(variables, record);
    return (Date) timeDriverElEval.eval(variables, timeDriver);
  }

  protected String getRecordKey(Record record) throws ELException {
    ELEvaluator.Variables variables = new ELEvaluator.Variables();
    ELRecordSupport.setRecordInContext(variables, record);
    return (String) keyElEval.eval(variables, keyEl);
  }

  protected Class<? extends CompressionCodec> getCompressionCodec() {
    return compressionCodec;
  }

  @Override
  public void destroy() {
    try {
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

  public abstract void processBatch(Batch batch) throws StageException;

}
