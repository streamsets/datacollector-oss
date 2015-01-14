/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hdfs;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import com.streamsets.pipeline.api.base.BaseTarget;

import java.util.Map;

public abstract class AbstractHdfsTarget extends BaseTarget {

  public enum FileType implements BaseEnumChooserValues.EnumWithLabel {
    TEXT("Text file"), SEQUENCE_FILE("Sequence file");

    private String label;
    FileType(String label) {
      this.label = label;
    }
    @Override
    public String getLabel() {
      return label;
    }

  }

  public enum Compression { NONE, GZIP, BZIP2, SNAPPY}

  public enum TimeFrom implements BaseEnumChooserValues.EnumWithLabel {
    DATA_COLLECTOR_CLOCK;

    @Override
    public String getLabel() {
      return "Use clock from Data Collector host";
    }

  }

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      description = "The URI of the Hadoop HDFS Cluster",
      label = "HDFS URI")
  public String hdfsUri;

  @ConfigDef(required = true,
      type = ConfigDef.Type.BOOLEAN,
      description = "Indicates if HDFS requires Kerberos authentication",
      label = "Kerberos Enabled",
      defaultValue = "false")
  public boolean hdfsKerberos;

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      description = "Kerberos principal used to connect to HDFS",
      label = "Kerberos principal",
      dependsOn = "hdfsKerberos", triggeredByValue = "true")
  public String kerberosPrincipal;

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      description = "The location of the Kerberos keytab file with credentials for the Kerberos principal",
      label = "Kerberos keytab file",
      dependsOn = "hdfsKerberos", triggeredByValue = "true")
  public String kerberosKeytab;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MAP,
      description = "Additional HDFS configuration to be used by the client (i.e. replication factor, rpc timeout, etc.)",
      label = "HDFS configs")
  public Map<String, String> hdfsConfig;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Timezone code used to resolve file paths",
      label = "Timezone",
      defaultValue = "UTC")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = TimeZoneChooserValues.class)
  public String timeZone;

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      description = "Path template for the HDFS files, the template supports the following time tokens: " +
                    "${YY}, ${YYYY}, ${MM}, ${DD}, ${hh}, ${mm}, ${ss}, ${fileCount}, ${record:value(<fieldpath>)}. " +
                    "IMPORTANT: the resulting file path must be globally unique to avoid multiple data collectors " +
                    "stepping on each other",
      label = "Path template")
  public String pathTemplate;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "File format type",
      label = "File type",
      defaultValue = "TEXT")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = FileTypeChooserValues.class)
  public FileType fileType;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Compression codec to use",
      label = "Compression codec",
      defaultValue = "NONE")
  @ValueChooser(type = ChooserMode.SUGGESTED, chooserValues = CompressionChooserValues.class)
  public String compression;

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      description = "Maximum number of records that trigger a file rollover. 0 does not trigger a rollover.",
      label = "Max records per File",
      defaultValue = "0")
  public long maxRecordsPerFile;

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      description = "Maximum file size in Megabytes that trigger a file rollover. 0 does not trigger a rollover",
      label = "Max file size (MBs)",
      defaultValue = "0")
  public long maxFileSize;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      description = "Indicates if the time should be driven by the Data Collector clock or by a time specified in " +
                    "a record field",
      label = "Use Time From",
      defaultValue = "DATA_COLLECTOR_CLOCK")
  @ValueChooser(type = ChooserMode.SUGGESTED, chooserValues = TimeFromChooserValues.class)
  public String useTimeFrom;

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      description = "Time limit (in seconds) for a record to be written to the default HDFS file, if the limit is " +
                    "exceeded the record will be written to the current late records file. 0 means no limit.",
      label = "Record Time Limit (Secs)",
      defaultValue = "0",
      dependsOn = "useTimeFrom", triggeredByValue = "DATA_COLLECTOR_CLOCK")
  public long cutoffTimeIntervalSec;

  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      description = "Path template for the HDFS files, the template supports the following time tokens: " +
                    "${YY}, ${YYYY}, ${MM}, ${DD}, ${hh}, ${mm}, ${ss}",
      defaultValue = " ",
      label = "Late Records Path Template",
      dependsOn = "useTimeFrom", triggeredByValue = "DATA_COLLECTOR_CLOCK")
  public String lateRecordsPathTemplate;

  @ConfigDef(required = false,
      type = ConfigDef.Type.MODEL,
      description = "File format type",
      label = "File type",
      defaultValue = "TEXT",
      dependsOn = "useTimeFrom", triggeredByValue = "DATA_COLLECTOR_CLOCK")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = FileTypeChooserValues.class)
  public FileType lateRecordsFileType;

  @ConfigDef(required = false,
      type = ConfigDef.Type.MODEL,
      description = "Compression codec to use",
      label = "Compression codec",
      defaultValue = "NONE",
      dependsOn = "useTimeFrom", triggeredByValue = "DATA_COLLECTOR_CLOCK")
  @ValueChooser(type = ChooserMode.SUGGESTED, chooserValues = CompressionChooserValues.class)
  public String lateRecordsCompression;

  @Override
  public void write(Batch batch) throws StageException {

  }

}
