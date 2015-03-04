/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfig;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.FileRawSourcePreviewer;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvHeaderChooserValues;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvModeChooserValues;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.DataFormatChooserValues;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.JsonModeChooserValues;
import com.streamsets.pipeline.configurablestage.DSource;

@GenerateResourceBundle
@StageDef(
    version = "1.0.0",
    label = "Directory",
    description = "Reads files from a directory",
    icon="spoolDirSource.png"
)
@RawSource(rawSourcePreviewer = FileRawSourcePreviewer.class)
@ConfigGroups(Groups.class)
@HideConfig(onErrorRecord = true)
public class SpoolDirDSource extends DSource {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      description = "Format of data in the files",
      displayPosition = 0,
      group = "FILES"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = DataFormatChooserValues.class)
  public DataFormat dataFormat;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Files Directory",
      description = "Use a local directory",
      displayPosition = 10,
      group = "FILES"
  )
  public String spoolDir;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.INTEGER,
      label = "Overrun Limit (KB)",
      defaultValue = "128",
      description = "Low level reader limit to avoid Out of Memory errors",
      displayPosition = 15,
      group = "FILES"
  )
  public int overrunLimit;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.INTEGER,
      label = "Batch Size (recs)",
      defaultValue = "1000",
      description = "Max number of records per batch",
      displayPosition = 20,
      group = "FILES"
  )
  public int batchSize;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.INTEGER,
      defaultValue = "600",
      label = "Batch Wait Time (secs)",
      description = "Max time to wait for new files before sending an empty batch",
      displayPosition = 30,
      group = "FILES"
  )
  public long poolingTimeoutSecs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "File Name Pattern",
      description = "A glob or regular expression that defines the pattern of the file names in the directory. " +
                    "Files are processed in naturally ascending order.",
      displayPosition = 40,
      group = "FILES",
      dependsOn = "dataFormat",
      triggeredByValue = { "TEXT", "JSON", "XML", "DELIMITED"}
  )
  public String filePattern;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.INTEGER,
      defaultValue = "10",
      label = "Max Files in Directory",
      description = "Max number of files in the directory waiting to be processed. Additional files cause the " +
                    "pipeline to fail.",
      displayPosition = 60,
      group = "FILES",
      dependsOn = "dataFormat",
      triggeredByValue = { "TEXT", "JSON", "XML", "DELIMITED"}
  )
  public int maxSpoolFiles;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "First File to Process",
      description = "When configured, the Data Collector does not process earlier (naturally ascending order) file names",
      displayPosition = 50,
      group = "FILES",
      dependsOn = "dataFormat",
      triggeredByValue = { "TEXT", "JSON", "XML", "DELIMITED"}
  )
  public String initialFileToProcess;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Error Directory",
      description = "Directory for files that could not be fully processed",
      displayPosition = 100,
      group = "POST_PROCESSING"
  )
  public String errorArchiveDir;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NONE",
      label = "File Post Processing",
      description = "Action to take after processing a file",
      displayPosition = 110,
      group = "POST_PROCESSING"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = PostProcessingOptionsChooserValues.class)
  public PostProcessingOptions postProcessing;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Archiving Directory",
      description = "Directory to archive files after they have been processed",
      displayPosition = 200,
      group = "POST_PROCESSING",
      dependsOn = "postProcessing",
      triggeredByValue = "ARCHIVE"
  )
  public String archiveDir;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.INTEGER,
      defaultValue = "0",
      label = "Archive Retention Time (minutes)",
      description = "How long archived files should be kept before deleting, a value of zero means forever",
      displayPosition = 210,
      group = "POST_PROCESSING",
      dependsOn = "postProcessing",
      triggeredByValue = "ARCHIVE"
  )
  public long retentionTimeMins;

  // CSV Configuration

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "CSV",
      label = "File Type",
      description = "",
      displayPosition = 300,
      group = "DELIMITED",
      dependsOn = "dataFormat",
      triggeredByValue = "DELIMITED"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = CsvModeChooserValues.class)
  public CsvMode csvFileFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NO_HEADER",
      label = "Header Line",
      description = "",
      displayPosition = 310,
      group = "DELIMITED",
      dependsOn = "dataFormat",
      triggeredByValue = "DELIMITED"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = CsvHeaderChooserValues.class)
  public CsvHeader csvHeader;

  // JSON Configuration

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "MULTIPLE_OBJECTS",
      label = "JSON Content",
      description = "",
      displayPosition = 400,
      group = "JSON",
      dependsOn = "dataFormat",
      triggeredByValue = "JSON"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = JsonModeChooserValues.class)
  public JsonMode jsonContent;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.INTEGER,
      defaultValue = "4096",
      label = "Max Object Length (chars)",
      description = "Larger objects are not processed",
      displayPosition = 410,
      group = "JSON",
      dependsOn = "dataFormat",
      triggeredByValue = "JSON"
  )
  public int maxJsonObjectLen;

  // LOG Configuration

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.INTEGER,
      defaultValue = "1024",
      label = "Max Line Length",
      description = "Longer lines are truncated",
      displayPosition = 500,
      group = "TEXT",
      dependsOn = "dataFormat",
      triggeredByValue = "TEXT"
  )
  public int maxLogLineLength;

  // XML Configuration

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Delimiter Element",
      description = "XML element that acts as a record delimiter",
      displayPosition = 600,
      group = "XML",
      dependsOn = "dataFormat",
      triggeredByValue = "XML"
  )
  public String xmlRecordElement;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.INTEGER,
      defaultValue = "4096",
      label = "Max Record Length (chars)",
      description = "Larger records are not processed",
      displayPosition = 610,
      group = "XML",
      dependsOn = "dataFormat",
      triggeredByValue = "XML"
  )
  public int maxXmlObjectLen;

  @Override
  protected Source createSource() {
    return new SpoolDirSource(dataFormat, overrunLimit, spoolDir, batchSize, poolingTimeoutSecs, filePattern,
                              maxSpoolFiles, initialFileToProcess, errorArchiveDir, postProcessing, archiveDir,
                              retentionTimeMins, csvFileFormat, csvHeader, jsonContent, maxJsonObjectLen,
                              maxLogLineLength, xmlRecordElement, maxXmlObjectLen);
  }

}
