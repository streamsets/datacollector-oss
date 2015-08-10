/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.el.SdcEL;
import com.streamsets.pipeline.config.CharsetChooserValues;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvModeChooserValues;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.JsonModeChooserValues;
import com.streamsets.pipeline.config.TimeZoneChooserValues;
import com.streamsets.pipeline.configurablestage.DTarget;
import com.streamsets.pipeline.lib.el.DataUtilEL;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;

import java.util.Map;

@StageDef(
    version = 1,
    label = "Hadoop FS",
    description = "Writes to a Hadoop file system",
    icon = "hdfs.png",
    privateClassLoader = true
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class HdfsDTarget extends DTarget {

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
      required = false,
      type = ConfigDef.Type.STRING,
      label = "HDFS User",
      description = "If set, the data collector will write to HDFS as this user. " +
                    "The data collector user must be configured as a proxy user in HDFS.",
      displayPosition = 20,
      group = "HADOOP_FS"
  )
  public String hdfsUser;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Kerberos Authentication",
      defaultValue = "false",
      description = "",
      displayPosition = 30,
      group = "HADOOP_FS"
  )
  public boolean hdfsKerberos;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Hadoop FS Configuration Directory",
      description = "An absolute path or a directory under SDC resources directory to load core-site.xml and hdfs-site.xml files " +
                    "to configure the Hadoop FileSystem.",
      displayPosition = 50,
      group = "HADOOP_FS"
  )
  public String hdfsConfDir;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Hadoop FS Configuration",
      description = "Additional Hadoop properties to pass to the underlying Hadoop FileSystem. These properties " +
                    "have precedence over properties loaded via the 'Hadoop FS Configuration Directory' property.",
      displayPosition = 60,
      group = "HADOOP_FS"
  )
  public Map<String, String> hdfsConfigs;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "sdc-${sdc:id()}",
      label = "Files Prefix",
      description = "File name prefix",
      displayPosition = 105,
      group = "OUTPUT_FILES",
      elDefs = SdcEL.class
  )
  public String uniquePrefix;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "/tmp/out/${YYYY()}-${MM()}-${DD()}-${hh()}",
      label = "Directory Template",
      description = "Template for the creation of output directories. Valid variables are ${YYYY()}, ${MM()}, ${DD()}, " +
                    "${hh()}, ${mm()}, ${ss()} and {record:value(“/field”)} for values in a field. Directories are " +
                    "created based on the smallest time unit variable used.",
      displayPosition = 110,
      group = "OUTPUT_FILES",
      elDefs = {RecordEL.class, TimeEL.class, ExtraTimeEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
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
  @ValueChooser(TimeZoneChooserValues.class)
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
      min = 0
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
      min = 0
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
  @ValueChooser(CompressionChooserValues.class)
  public CompressionMode compression;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Compression Codec Class",
      description = "Use the full class name",
      displayPosition = 170,
      group = "OUTPUT_FILES",
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
      group = "OUTPUT_FILES"
  )
  @ValueChooser(FileTypeChooserValues.class)
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
      group = "OUTPUT_FILES",
      dependsOn = "fileType",
      triggeredByValue = "SEQUENCE_FILE"
  )
  @ValueChooser(HdfsSequenceFileCompressionTypeChooserValues.class)
  public HdfsSequenceFileCompressionType seqFileCompressionType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "${1 * HOURS}",
      label = "Late Record Time Limit (secs)",
      description = "Time limit (in seconds) for a record to be written to the corresponding HDFS directory, if the " +
                    "limit is exceeded the record will be written to the current late records file. 0 means no limit. " +
                    "If a number is used it is considered seconds, it can be multiplied by 'MINUTES' or 'HOURS', ie: " +
                    "'${30 * MINUTES}'",
      displayPosition = 200,
      group = "LATE_RECORDS",
      elDefs = {TimeEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String lateRecordsLimit;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "SEND_TO_ERROR",
      label = "Late Record Handling",
      description = "Action for records considered late.",
      displayPosition = 210,
      group = "LATE_RECORDS"
  )
  @ValueChooser(LateRecordsActionChooserValues.class)
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
      elDefs = {RecordEL.class, TimeEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
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
  @ValueChooser(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "UTF-8",
    label = "Data  Charset",
    displayPosition = 105,
    group = "OUTPUT_FILES",
    dependsOn = "dataFormat",
    triggeredByValue = {"TEXT", "JSON", "DELIMITED", "XML", "LOG"}
  )
  @ValueChooser(CharsetChooserValues.class)
  public String charset;

  /********  For DELIMITED ***********/

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "CSV",
      label = "CSV Format",
      description = "",
      displayPosition = 310,
      group = "DELIMITED",
      dependsOn = "dataFormat",
      triggeredByValue = "DELIMITED"
  )
  @ValueChooser(CsvModeChooserValues.class)
  public CsvMode csvFileFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NO_HEADER",
      label = "Header Line",
      description = "",
      displayPosition = 320,
      group = "DELIMITED",
      dependsOn = "dataFormat",
      triggeredByValue = "DELIMITED"
  )
  @ValueChooser(HdfsCsvHeaderChooserValues.class)
  public CsvHeader csvHeader;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Remove New Line Characters",
      description = "Replaces new lines characters with white spaces",
      displayPosition = 330,
      group = "DELIMITED",
      dependsOn = "dataFormat",
      triggeredByValue = "DELIMITED"
  )
  public boolean csvReplaceNewLines;

  /********  For JSON *******/

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "MULTIPLE_OBJECTS",
      label = "JSON Content",
      description = "",
      displayPosition = 200,
      group = "JSON",
      dependsOn = "dataFormat",
      triggeredByValue = "JSON"
  )
  @ValueChooser(JsonModeChooserValues.class)
  public JsonMode jsonMode;

  /********  For TEXT *******/

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "/text",
      label = "Text Field Path",
      description = "",
      displayPosition = 300,
      group = "TEXT",
      dependsOn = "dataFormat",
      triggeredByValue = "TEXT"
  )
  public String textFieldPath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Empty Line If No Text",
      description = "",
      displayPosition = 310,
      group = "TEXT",
      dependsOn = "dataFormat",
      triggeredByValue = "TEXT"
  )
  public boolean textEmptyLineIfNull;

  /********  For AVRO *******/

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.TEXT,
    defaultValue = "",
    label = "Avro Schema",
    description = "Optionally use the runtime:loadResource function to use a schema stored in a file",
    displayPosition = 320,
    group = "AVRO",
    dependsOn = "dataFormat",
    triggeredByValue = {"AVRO"},
    mode = ConfigDef.Mode.JSON
  )
  public String avroSchema;

  @Override
  protected Target createTarget() {
    return new HdfsTarget(
      hdfsUri,
      hdfsUser,
      hdfsKerberos,
      hdfsConfDir,
      hdfsConfigs,
      uniquePrefix,
      dirPathTemplate,
      timeZoneID,
      timeDriver,
      maxRecordsPerFile,
      maxFileSize,
      compression,
      otherCompression,
      fileType,
      keyEl,
      seqFileCompressionType,
      lateRecordsLimit,
      lateRecordsAction,
      lateRecordsDirPathTemplate,
      dataFormat,
      charset,
      csvFileFormat,
      csvHeader,
      csvReplaceNewLines,
      jsonMode,
      textFieldPath,
      textEmptyLineIfNull,
      avroSchema
    );
  }

}
