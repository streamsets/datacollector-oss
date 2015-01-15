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
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import com.streamsets.pipeline.api.base.BaseTarget;

import java.util.Map;

@ConfigGroups(HdfsConfigGroups.class)
public abstract class BaseHdfsTarget extends BaseTarget {

  public enum Compression { NONE, GZIP, BZIP2, SNAPPY}

//  TODO: SCD-65
//  public enum TimeFrom implements BaseEnumChooserValues.EnumWithLabel {
//    DATA_COLLECTOR_CLOCK;
//
//    @Override
//    public String getLabel() {
//      return "Use clock from Data Collector host";
//    }
//
//  }

  public enum HdfsLateRecordsAction implements BaseEnumChooserValues.EnumWithLabel {
    SEND_TO_ERROR("Send to error"),
    SEND_TO_LATE_RECORDS_FILE("Send to late records file"),
    DISCARD("Discard"),

    ;
    private final String label;
    HdfsLateRecordsAction(String label) {
      this.label = label;
    }

    @Override
    public String getLabel() {
      return label;
    }
  }

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
  public Map<String, String> hdfsConfig;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Timezone code used to resolve file paths",
      label = "Timezone",
      defaultValue = "UTC",
      group = "FILE",
      displayPosition = 100)
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = TimeZoneChooserValues.class)
  public String timeZone;

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      description = "Path template for the HDFS files, the template supports the following time tokens: " +
                    "${YY}, ${YYYY}, ${MM}, ${DD}, ${hh}, ${mm}, ${ss}, ${fileCount}, ${record:value(<fieldpath>)}. " +
                    "IMPORTANT: the resulting file path must be globally unique to avoid multiple data collectors " +
                    "stepping on each other",
      label = "Path Template",
      group = "FILE"
  )
  public String pathTemplate;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "File type to create in HDFS",
      label = "File Type",
      defaultValue = "TEXT",
      group = "FILE",
      displayPosition = 101)
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = FileTypeChooserValues.class)
  public HdfsFileType fileType;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Compression codec to use",
      label = "Compression Codec",
      defaultValue = "NONE",
      group = "FILE",
      displayPosition = 102)
  @ValueChooser(type = ChooserMode.SUGGESTED, chooserValues = CompressionChooserValues.class)
  public String compression;

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      description = "Maximum number of records that trigger a file rollover. 0 does not trigger a rollover.",
      label = "Max records per File",
      defaultValue = "0",
      group = "FILE",
      displayPosition = 103)
  public long maxRecordsPerFile;

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      description = "Maximum file size in Megabytes that trigger a file rollover. 0 does not trigger a rollover",
      label = "Max file size (MBs)",
      defaultValue = "0",
      group = "FILE",
      displayPosition = 104)
  public long maxFileSize;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Indicates if the time should be driven by the Data Collector clock or by a time specified in " +
                    "a record field",
      label = "Use Time From",
      defaultValue = "DATA_COLLECTOR_CLOCK",
      group = "FILE",
      displayPosition = 105)
  @ValueChooser(type = ChooserMode.SUGGESTED, chooserValues = TimeFromChooserValues.class)
  public String useTimeFrom;

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      description = "Time limit (in seconds) for a record to be written to the default HDFS file, if the limit is " +
                    "exceeded the record will be written to the current late records file. 0 means no limit.",
      label = "Late Records Time Limit (Secs)",
      defaultValue = "0",
      group = "LATE_RECORDS",
      dependsOn = "useTimeFrom",
      triggeredByValue = "DATA_COLLECTOR_CLOCK",
      displayPosition = 106)
  public long cutoffTimeIntervalSec;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Indicates if the time should be driven by the Data Collector clock or by a time specified in " +
                    "a record field",
      label = "Late Records",
      defaultValue = "SEND_TO_ERROR",
      group = "LATE_RECORDS",
      dependsOn = "useTimeFrom",
      triggeredByValue = "DATA_COLLECTOR_CLOCK",
      displayPosition = 107)
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = LateRecordsActionChooserValues.class)
  public HdfsLateRecordsAction lateRecordsAction;

  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      description = "Path template for the HDFS files for late records, the template supports the following time tokens: " +
                    "${YY}, ${YYYY}, ${MM}, ${DD}, ${hh}, ${mm}, ${ss}. IMPORTANT: Used only if 'Late Records' is " +
                    "set to 'Send to late records file'.",
      defaultValue = "",
      group = "LATE_RECORDS",
      label = "Late Records Path Template",
      dependsOn = "useTimeFrom",
      triggeredByValue = "DATA_COLLECTOR_CLOCK",
      displayPosition = 108)
  public String lateRecordsPathTemplate;

  @Override
  public void write(Batch batch) throws StageException {

  }

}
