/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.hdfs;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.el.SdcEL;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.TimeZoneChooserValues;
import com.streamsets.pipeline.lib.el.DataUtilEL;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.hdfs.common.Errors;
import com.streamsets.pipeline.lib.hdfs.common.HdfsBaseConfigBean;
import com.streamsets.pipeline.stage.destination.hdfs.writer.ActiveRecordWriters;
import com.streamsets.pipeline.stage.destination.hdfs.writer.RecordWriterManager;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class HdfsTargetConfigBean extends HdfsBaseConfigBean {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsTargetConfigBean.class);
  private static final int MEGA_BYTE = 1024 * 1024;

  @Override
  protected String getConfigBeanPrefix() {
    return "hdfsTargetConfigBean.";
  }

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    defaultValue = "sdc-${sdc:id()}",
    label = "Files Prefix",
    description = "File name prefix",
    displayPosition = 105,
    group = "OUTPUT_FILES",
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    elDefs = SdcEL.class
  )
  public String uniquePrefix;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Files Suffix",
      description = "File name suffix e.g.'txt'",
      displayPosition = 106,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "OUTPUT_FILES",
      dependsOn = "fileType",
      triggeredByValue = {"TEXT", "SEQUENCE_FILE"}
  )
  public String fileNameSuffix;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Directory in Header",
    description = "The directory is defined by the '" + HdfsTarget.TARGET_DIRECTORY_HEADER + "' record header attribute instead of the Directory Template configuration property.",
    displayPosition = 107,
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    group = "OUTPUT_FILES"
  )
  public boolean dirPathTemplateInHeader;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "/tmp/out/${YYYY()}-${MM()}-${DD()}-${hh()}",
    label = "Directory Template",
    description = "Template for the creation of output directories. Valid variables are ${YYYY()}, ${MM()}, ${DD()}, " +
      "${hh()}, ${mm()}, ${ss()} and {record:value(“/field”)} for values in a field. Directories are " +
      "created based on the smallest time unit variable used.",
    displayPosition = 110,
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "OUTPUT_FILES",
    elDefs = {RecordEL.class, TimeEL.class, ExtraTimeEL.class},
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    dependsOn = "dirPathTemplateInHeader",
    triggeredByValue = "false"
  )
  public String dirPathTemplate;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "UTC",
    label = "Data Time Zone",
    description = "Time zone to use to resolve directory paths",
    displayPosition = 120,
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    group = "OUTPUT_FILES"
  )
  @ValueChooserModel(TimeZoneChooserValues.class)
  public String timeZoneID;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "${time:now()}",
    label = "Time Basis",
    description = "Time basis to use for a record. Enter an expression that evaluates to a datetime. To use the " +
      "processing time, enter ${time:now()}. To use field values, use '${record:value(\"<filepath>\")}'.",
    displayPosition = 130,
    group = "OUTPUT_FILES",
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
    evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String timeDriver;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "0",
    label = "Max Records in File",
    description = "Number of records that triggers the creation of a new file. Use 0 to opt out.",
    displayPosition = 140,
    group = "OUTPUT_FILES",
    min = 0,
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    dependsOn = "fileType",
    triggeredByValue = {"TEXT", "SEQUENCE_FILE"}
  )
  public long maxRecordsPerFile;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "0",
    label = "Max File Size (MB)",
    description = "Exceeding this size triggers the creation of a new file. Use 0 to opt out.",
    displayPosition = 150,
    group = "OUTPUT_FILES",
    min = 0,
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    dependsOn = "fileType",
    triggeredByValue = {"TEXT", "SEQUENCE_FILE"}
  )
  public long maxFileSize;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "${1 * HOURS}",
    label = "Idle Timeout",
    description = "Maximum time for a file to remain idle. After no records are written to a file for the" +
      " specified time, the destination closes the file. Enter a number to specify a value in seconds. You" +
      " can also use the MINUTES or HOURS constants in an expression. Use -1 to opt out of a timeout.",
    group = "OUTPUT_FILES",
    displayPosition = 155,
    elDefs = {TimeEL.class},
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    dependsOn = "fileType",
    triggeredByValue = {"TEXT", "SEQUENCE_FILE"}
  )
  public String idleTimeout;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "NONE",
    label = "Compression Codec",
    description = "",
    displayPosition = 160,
    group = "OUTPUT_FILES",
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    dependsOn = "fileType",
    triggeredByValue = {"TEXT", "SEQUENCE_FILE"}
  )
  @ValueChooserModel(CompressionChooserValues.class)
  public CompressionMode compression;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "",
    label = "Compression Codec Class",
    description = "Use the full class name",
    displayPosition = 170,
    group = "OUTPUT_FILES",
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    dependsOn = "compression",
    triggeredByValue = "OTHER"
  )

  public String otherCompression;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "TEXT",
    label = "File Type",
    description = "",
    displayPosition = 100,
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    group = "OUTPUT_FILES"
  )
  @ValueChooserModel(FileTypeChooserValues.class)
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
    triggeredByValue = "SEQUENCE_FILE",
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    elDefs = {RecordEL.class, DataUtilEL.class}
  )
  public String keyEl;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "BLOCK",
    label = "Compression Type",
    description = "Compression type if using a CompressionCodec",
    displayPosition = 190,
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    group = "OUTPUT_FILES",
    dependsOn = "fileType",
    triggeredByValue = "SEQUENCE_FILE"
  )
  @ValueChooserModel(HdfsSequenceFileCompressionTypeChooserValues.class)
  public HdfsSequenceFileCompressionType seqFileCompressionType;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "${1 * HOURS}",
    label = "Late Record Time Limit (secs)",
    description = "Time limit (in seconds) for a record to be written to the corresponding directory, if the " +
      "limit is exceeded the record will be written to the current late records file. " +
      "If a number is used it is considered seconds, it can be multiplied by 'MINUTES' or 'HOURS', ie: " +
      "'${30 * MINUTES}'",
    displayPosition = 200,
    group = "LATE_RECORDS",
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    elDefs = {TimeEL.class},
    evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String lateRecordsLimit;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Use Roll Attribute",
    description = "Closes the current file and creates a new file when processing a record with the specified roll attribute",
    displayPosition = 204,
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    group = "OUTPUT_FILES"
  )
  public boolean rollIfHeader;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "roll",
    label = "Roll Attribute Name",
    description = "Name of the roll attribute",
    displayPosition = 205,
    group = "OUTPUT_FILES",
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    dependsOn = "rollIfHeader",
    triggeredByValue = "true"
  )
  public String rollHeaderName;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "SEND_TO_ERROR",
    label = "Late Record Handling",
    description = "Action for records considered late.",
    displayPosition = 210,
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    group = "LATE_RECORDS"
  )
  @ValueChooserModel(LateRecordsActionChooserValues.class)
  public LateRecordsAction lateRecordsAction;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    defaultValue = "/tmp/late/${YYYY()}-${MM()}-${DD()}",
    label = "Late Record Directory Template",
    description = "Template for the creation of late record directories. Valid variables are ${YYYY()}, ${MM()}, " +
      "${DD()}, ${hh()}, ${mm()}, ${ss()}.",
    displayPosition = 220,
    group = "LATE_RECORDS",
    dependsOn = "lateRecordsAction",
    triggeredByValue = "SEND_TO_LATE_RECORDS_FILE",
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    elDefs = {RecordEL.class, TimeEL.class},
    evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String lateRecordsDirPathTemplate;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    label = "Data Format",
    description = "Data Format",
    displayPosition = 1,
    group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "true",
    label = "Validate Permissions",
    description = "When checked, the destination creates a test file in configured target directory to verify access privileges.",
    displayPosition = 230,
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    group = "OUTPUT_FILES"
  )
  public boolean hdfsPermissionCheck;

  //Optional if empty file is created with default umask.
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      elDefs = {RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      label = "Permissions Expression",
      description = "Expression that determines the target file permissions." +
          "Should be a octal/symbolic representation of the permissions.",
      displayPosition = 460,
      group = "DATA_FORMAT",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependsOn = "dataFormat",
      triggeredByValue = "WHOLE_FILE"
  )
  public String permissionEL = "";

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    label = "Skip file recovery",
    defaultValue = "false",
    description = "Set to true to skip finding old temporary files that were written to and automatically recover them.",
    displayPosition = 1000,
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    group = "OUTPUT_FILES"
  )
  public boolean skipOldTempFileRecovery = false;

  @ConfigDefBean(groups = {"DATA_FORMAT"})
  public DataGeneratorFormatConfig dataGeneratorFormatConfig;

  //private members

  private long lateRecordsLimitSecs;
  private long idleTimeSecs = -1;
  private ActiveRecordWriters currentWriters;
  private ActiveRecordWriters lateWriters;
  private ELEval timeDriverElEval;
  private CompressionCodec compressionCodec;
  private Counter toHdfsRecordsCounter;
  private Meter toHdfsRecordsMeter;
  private Counter lateRecordsCounter;
  private Meter lateRecordsMeter;

  //public API

  public void init(final Stage.Context context, List<Stage.ConfigIssue> issues) {
    boolean hadoopFSValidated = validateHadoopFS(context, issues);
    String fileNameEL = "";

    lateRecordsLimitSecs =
        initTimeConfigs(context, "lateRecordsLimit", lateRecordsLimit, Groups.LATE_RECORDS,
            false, Errors.HADOOPFS_10, issues);
    if (idleTimeout != null && !idleTimeout.isEmpty()) {
      idleTimeSecs = initTimeConfigs(context, "idleTimeout", idleTimeout, Groups.OUTPUT_FILES,
          true, Errors.HADOOPFS_52, issues);
    }
    if (maxFileSize < 0) {
      issues.add(
          context.createConfigIssue(
              Groups.LATE_RECORDS.name(),
              getConfigBeanPrefix() + "maxFileSize",
              Errors.HADOOPFS_08
          )
      );
    }

    if (maxRecordsPerFile < 0) {
      issues.add(
          context.createConfigIssue(
              Groups.LATE_RECORDS.name(),
              getConfigBeanPrefix() + "maxRecordsPerFile",
              Errors.HADOOPFS_09
          )
      );
    }

    if (uniquePrefix == null) {
      uniquePrefix = "";
    }

    if (fileNameSuffix == null) {
      fileNameSuffix = "";
    } else {
      //File Suffix should not contain '/' or start with '.'
      if(fileType != HdfsFileType.WHOLE_FILE && (fileNameSuffix.startsWith(".") || fileNameSuffix.contains("/"))) {
        issues.add(
            context.createConfigIssue(
                Groups.HADOOP_FS.name(),
                getConfigBeanPrefix() + "fileNameSuffix",
                Errors.HADOOPFS_57
            )
        );
      }
    }

    dataGeneratorFormatConfig.init(
        context,
        dataFormat,
        Groups.OUTPUT_FILES.name(),
        getConfigBeanPrefix() + "dataGeneratorFormatConfig",
        issues
    );

    if (dataFormat == DataFormat.WHOLE_FILE || fileType == HdfsFileType.WHOLE_FILE) {
      validateStageForWholeFileFormat(context, issues);
      fileNameEL = dataGeneratorFormatConfig.fileNameEL;
    }

    SequenceFile.CompressionType compressionType = (seqFileCompressionType != null)
      ? seqFileCompressionType.getType() : null;
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
            throw new StageException(Errors.HADOOPFS_05, otherCompression, ex1.toString(), ex1);
          }
          break;
        case NONE:
          break;
        default:
          try {
            compressionCodec = compression.getCodec().newInstance();
          } catch (IllegalAccessException | InstantiationException ex) {
            LOG.info("Error: " + ex.getMessage(), ex.toString(), ex);
            issues.add(context.createConfigIssue(Groups.OUTPUT_FILES.name(), null, Errors.HADOOPFS_48, ex.toString(), ex));
          }
          break;
      }
      if (compressionCodec != null) {
        if (compressionCodec instanceof Configurable) {
          ((Configurable) compressionCodec).setConf(hdfsConfiguration);
        }
      }
    } catch (StageException ex) {
      LOG.info("Validation Error: " + ex.getMessage(), ex.toString(), ex);
      issues.add(context.createConfigIssue(Groups.OUTPUT_FILES.name(), null, ex.getErrorCode(), ex.toString(), ex));
    }

    if(hadoopFSValidated){
      try {
        // Creating RecordWriterManager for dirPathTemplate
        RecordWriterManager mgr = new RecordWriterManager(
            fs,
            hdfsConfiguration,
            uniquePrefix,
            fileNameSuffix,
            dirPathTemplateInHeader,
            dirPathTemplate,
            TimeZone.getTimeZone(ZoneId.of(timeZoneID)),
            lateRecordsLimitSecs,
            maxFileSize * MEGA_BYTE,
            maxRecordsPerFile,
            fileType,
            compressionCodec,
            compressionType,
            keyEl,
            rollIfHeader,
            rollHeaderName,
            fileNameEL,
            dataGeneratorFormatConfig.wholeFileExistsAction,
            permissionEL,
            dataGeneratorFormatConfig.getDataGeneratorFactory(),
            (Target.Context) context,
            "dirPathTemplate"
        );

        if (idleTimeSecs > 0) {
          mgr.setIdleTimeoutSeconds(idleTimeSecs);
        }

        // We're skipping all hdfs-target-directory related validations if we're getting the configuration from header
        if(dirPathTemplateInHeader) {
          currentWriters = new ActiveRecordWriters(mgr);
        } else {
          // validate if the dirPathTemplate can be resolved by Els constants
          if (mgr.validateDirTemplate(
            Groups.OUTPUT_FILES.name(),
            "dirPathTemplate",
              getConfigBeanPrefix() + "dirPathTemplate",
            issues
          )) {
            String newDirPath = mgr.getDirPath(new Date()).toString();
            if (validateHadoopDir(       // permission check on the output directory
              context,
                getConfigBeanPrefix() + "dirPathTemplate",
              Groups.OUTPUT_FILES.name(),
              newDirPath, issues
            )) {
              currentWriters = new ActiveRecordWriters(mgr);
            }
          }
        }
      }  catch (Exception ex) {
        LOG.info("Validation Error: " + Errors.HADOOPFS_11.getMessage(), ex.toString(), ex);
        issues.add(context.createConfigIssue(Groups.OUTPUT_FILES.name(), null, Errors.HADOOPFS_11, ex.toString(), ex));
      }

      // Creating RecordWriterManager for Late Records
      if(lateRecordsDirPathTemplate != null && !lateRecordsDirPathTemplate.isEmpty()) {
        try {
          RecordWriterManager mgr = new RecordWriterManager(
                  fs,
                  hdfsConfiguration,
                  uniquePrefix,
                  fileNameSuffix,
                  false, // Late records doesn't support "template directory" to be in header
                  lateRecordsDirPathTemplate,
                  TimeZone.getTimeZone(ZoneId.of(timeZoneID)),
                  lateRecordsLimitSecs,
                  maxFileSize * MEGA_BYTE,
                  maxRecordsPerFile,
                  fileType,
                  compressionCodec,
                  compressionType,
                  keyEl,
                  false,
                  null,
                  fileNameEL,
                  dataGeneratorFormatConfig.wholeFileExistsAction,
                  permissionEL,
                  dataGeneratorFormatConfig.getDataGeneratorFactory(),
                  (Target.Context) context, "lateRecordsDirPathTemplate"
          );

          if (idleTimeSecs > 0) {
            mgr.setIdleTimeoutSeconds(idleTimeSecs);
          }

          // validate if the lateRecordsDirPathTemplate can be resolved by Els constants
          if (mgr.validateDirTemplate(
              Groups.OUTPUT_FILES.name(),
              "lateRecordsDirPathTemplate",
              getConfigBeanPrefix() + "lateRecordsDirPathTemplate",
              issues
          )) {
            String newLateRecordPath = mgr.getDirPath(new Date()).toString();
            if (lateRecordsAction == LateRecordsAction.SEND_TO_LATE_RECORDS_FILE &&
                lateRecordsDirPathTemplate != null && !lateRecordsDirPathTemplate.isEmpty() &&
                validateHadoopDir(       // permission check on the late record directory
                    context,
                    getConfigBeanPrefix() + "lateRecordsDirPathTemplate",
                    Groups.LATE_RECORDS.name(),
                    newLateRecordPath, issues
            )) {
              lateWriters = new ActiveRecordWriters(mgr);
            }
          }
        } catch (Exception ex) {
          issues.add(context.createConfigIssue(Groups.LATE_RECORDS.name(), null, Errors.HADOOPFS_17,
              ex.toString(), ex));
        }
      }
    }

    timeDriverElEval = context.createELEval("timeDriver");
    try {
      ELVars variables = context.createELVars();
      RecordEL.setRecordInContext(variables, context.createRecord("validationConfigs"));
      TimeNowEL.setTimeNowInContext(variables, new Date());
      context.parseEL(timeDriver);
      timeDriverElEval.eval(variables, timeDriver, Date.class);
    } catch (ELEvalException ex) {
      issues.add(
          context.createConfigIssue(
              Groups.OUTPUT_FILES.name(),
              getConfigBeanPrefix() + "timeDriver",
              Errors.HADOOPFS_19,
              ex.toString(),
              ex
          )
      );
    }

    if(rollIfHeader && (rollHeaderName == null || rollHeaderName.isEmpty())) {
      issues.add(
        context.createConfigIssue(
          Groups.OUTPUT_FILES.name(),
            getConfigBeanPrefix() + "rollHeaderName",
          Errors.HADOOPFS_51
        )
      );
    }

    if (issues.isEmpty()) {

      try {
        userUgi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            getCurrentWriters().commitOldFiles(fs);
            if (getLateWriters() != null) {
              getLateWriters().commitOldFiles(fs);
            }
            return null;
          }
        });
      } catch (Exception ex) {
        LOG.error("Exception while initializing bean configuration", ex);
        issues.add(context.createConfigIssue(null, null, Errors.HADOOPFS_23, ex.toString(), ex));
      }
      toHdfsRecordsCounter = context.createCounter("toHdfsRecords");
      toHdfsRecordsMeter = context.createMeter("toHdfsRecords");
      lateRecordsCounter = context.createCounter("lateRecords");
      lateRecordsMeter = context.createMeter("lateRecords");
    }

    if (issues.isEmpty()) {
      try {
        // Recover previously written files (promote all _tmp_ to their final form).
        //
        // We want to run the recovery only if
        // * Not preview
        // * This is not a WHOLE_FILE since it doesn't make sense there (tmp files will be discarded instead)
        // * User explicitly did not disabled the recovery in configuration
        // * We do have the directory template available (e.g. it's not in header)
        // * Only for the first runner, since it would be empty operation for the others
        recoveryOldTempFile(context);
      } catch (Exception ex) {
        LOG.error(Errors.HADOOPFS_59.getMessage(), ex.toString(), ex);
        issues.add(
            context.createConfigIssue(
              Groups.OUTPUT_FILES.name(),
                getConfigBeanPrefix() + "dirPathTemplate",
              Errors.HADOOPFS_59,
              ex.toString(),
              ex
            )
        );
      }
    }
  }

  public void destroy() {
    LOG.info("Destroy");
    try {
      if(userUgi != null) {
        userUgi.doAs((PrivilegedExceptionAction<Void>) () -> {
          try {
            //Don't close the whole files on destroy, we should only do it after
            //the file is copied (i.e after a record is written, the file will be closed)
            //For resume cases(i.e file copied fully but not renamed/ file partially copied)
            //we will overwrite the _tmp file and start copying from scratch
            if (currentWriters != null) {
              if (dataFormat != DataFormat.WHOLE_FILE) {
                currentWriters.closeAll();
              }
              currentWriters.getWriterManager().issueCachedEvents();
            }
            if (lateWriters != null) {
              if (dataFormat != DataFormat.WHOLE_FILE) {
                lateWriters.closeAll();
              }
              lateWriters.getWriterManager().issueCachedEvents();
            }
          } finally {
            if(fs != null) {
              fs.close();
              fs = null;
            }
          }
          return null;
        });
      }
    } catch (Exception ex) {
      LOG.warn("Error while closing FileSystem URI='{}': {}", hdfsUri, ex.toString(), ex);
    }
  }

  private long initTimeConfigs(
      Stage.Context context,
      String configName,
      String configuredValue,
      Groups configGroup,
      boolean allowNegOne,
      Errors errorCode,
      List<Stage.ConfigIssue> issues) {
    long timeInSecs = 0;
    try {
      ELEval timeEvaluator = context.createELEval(configName);
      context.parseEL(configuredValue);
      timeInSecs = timeEvaluator.eval(context.createELVars(),
          configuredValue, Long.class);
      if (timeInSecs <= 0 && (!allowNegOne || timeInSecs != -1)) {
        issues.add(
            context.createConfigIssue(
                configGroup.name(),
                getConfigBeanPrefix() + configName,
                errorCode
            )
        );
      }
    } catch (Exception ex) {
      issues.add(
          context.createConfigIssue(
              configGroup.name(),
              getConfigBeanPrefix() + configName,
              Errors.HADOOPFS_06,
              configName,
              configuredValue,
              ex.toString(),
              ex
          )
      );
    }
    return timeInSecs;
  }
  Counter getToHdfsRecordsCounter() {
    return toHdfsRecordsCounter;
  }

  Meter getToHdfsRecordsMeter() {
    return toHdfsRecordsMeter;
  }

  Counter getLateRecordsCounter() {
    return lateRecordsCounter;
  }

  Meter getLateRecordsMeter() {
    return lateRecordsMeter;
  }

  String getTimeDriver() {
    return timeDriver;
  }

  ELEval getTimeDriverElEval() {
    return timeDriverElEval;
  }

  UserGroupInformation getUGI() {
    return userUgi;
  }

  protected ActiveRecordWriters getCurrentWriters() {
    return currentWriters;
  }

  protected ActiveRecordWriters getLateWriters() {
    return lateWriters;
  }

  @VisibleForTesting
  Configuration getHdfsConfiguration() {
    return hdfsConfiguration;
  }

  @VisibleForTesting
  CompressionCodec getCompressionCodec() throws StageException {
    return compressionCodec;
  }

  @VisibleForTesting
  long getLateRecordLimitSecs() {
    return lateRecordsLimitSecs;
  }

  //private implementation

  protected void validateStageForWholeFileFormat(Stage.Context context, List<Stage.ConfigIssue> issues) {
    maxFileSize = 0;
    maxRecordsPerFile = 1;
    idleTimeout = "-1";
    if (fileType != HdfsFileType.WHOLE_FILE) {
      issues.add(
          context.createConfigIssue(
              Groups.OUTPUT_FILES.name(),
              getConfigBeanPrefix() + "fileType",
              Errors.HADOOPFS_53,
              fileType,
              HdfsFileType.WHOLE_FILE.getLabel(),
              DataFormat.WHOLE_FILE.getLabel()
          )
      );
    }
    if (dataFormat != DataFormat.WHOLE_FILE) {
      issues.add(
          context.createConfigIssue(
              Groups.DATA_FORMAT.name(),
              getConfigBeanPrefix() + "dataFormat",
              Errors.HADOOPFS_60,
              dataFormat.name(),
              DataFormat.WHOLE_FILE.getLabel(),
              HdfsFileType.WHOLE_FILE.getLabel()
          )
      );
    }
  }

  protected boolean validateHadoopDir(final Stage.Context context, final String configName, final String configGroup,
      String dirPathTemplate, final List<Stage.ConfigIssue> issues) {
    if (!dirPathTemplate.startsWith("/")) {
      issues.add(context.createConfigIssue(configGroup, configName, Errors.HADOOPFS_40));
      return false;
    }

    // User can opt out canary write to HDFS
    if(!hdfsPermissionCheck) {
      return true;
    }

    final AtomicBoolean ok = new AtomicBoolean(true);
    dirPathTemplate = (dirPathTemplate.isEmpty()) ? "/" : dirPathTemplate;
    try {
      final Path dir = new Path(dirPathTemplate);
      userUgi.doAs(new PrivilegedExceptionAction<Void>() {
                  @Override
                  public Void run() throws Exception {
          // Based on whether the target directory exists or not, we'll do different check
          if (!fs.exists(dir)) {
            // Target directory doesn't exists, we'll try to create directory a directory and then drop it
            Path workDir = dir;

            // We don't want to pollute HDFS with random directories, so we'll create exactly one directory under
            // another already existing directory on the template path. (e.g. if template is /a/b/c/d and only /a
            // exists, then we will create new dummy directory in /a during this test).
            while(!fs.exists(workDir)) {
              workDir = workDir.getParent();
            }

            // Sub-directory to be created in existing directory
            workDir = new Path(workDir, "_sdc-dummy-" + UUID.randomUUID().toString());

            try {
              if (fs.mkdirs(workDir)) {
                LOG.info("Creating dummy directory to validate permissions {}", workDir.toString());
                fs.delete(workDir, true);
                ok.set(true);
              } else {
                issues.add(context.createConfigIssue(configGroup, configName, Errors.HADOOPFS_41));
                ok.set(false);
              }
            } catch (IOException ex) {
              issues.add(context.createConfigIssue(configGroup, configName, Errors.HADOOPFS_42,
                  ex.toString()));
              ok.set(false);
            }
          } else {
            // Target directory exists, we will just create empty test file and then immediately drop it
            try {
              Path dummy = new Path(dir, "_sdc-dummy-" + UUID.randomUUID().toString());
              fs.create(dummy).close();
              fs.delete(dummy, false);
              ok.set(true);
            } catch (IOException ex) {
              issues.add(context.createConfigIssue(configGroup, configName, Errors.HADOOPFS_43,
                  ex.toString()));
              ok.set(false);
            }
          }
          return null;
      }
    });
    } catch (Exception ex) {
      issues.add(context.createConfigIssue(configGroup, configName, Errors.HADOOPFS_44,
        ex.toString()));
      ok.set(false);
    }

    return ok.get();
  }

  @Override
  protected FileSystem createFileSystem() throws Exception {
    try {
      return userUgi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return FileSystem.newInstance(new URI(hdfsUri), hdfsConfiguration);
        }
      });
    } catch (IOException ex) {
      throw ex;
    } catch (RuntimeException ex) {
      Throwable cause = ex.getCause();
      if (cause instanceof Exception) {
        throw (Exception)cause;
      }
      throw ex;
    }
  }

  private void recoveryOldTempFile(Stage.Context context) throws IOException, InterruptedException {
    if(!context.isPreview() && dataFormat != DataFormat.WHOLE_FILE && !skipOldTempFileRecovery && !dirPathTemplateInHeader && context.getRunnerId() == 0) {
      userUgi.doAs((PrivilegedExceptionAction<Void>) () -> {
        getCurrentWriters().getWriterManager().handleAlreadyExistingFiles();
        return null;
      });
    }
  }
}
