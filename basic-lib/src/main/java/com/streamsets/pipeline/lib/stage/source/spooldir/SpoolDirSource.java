/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.FileRawSourcePreviewer;

import java.io.File;

@GenerateResourceBundle
@StageDef(version = "1.0.0",
    label = "Directory",
    description = "Reads files from the specified directory. Files data can be: LOG, CSV, TSV, XML or JSON",
    icon="spoolDirSource.png")
@ConfigGroups(FileDataType.class)
@RawSource(rawSourcePreviewer = FileRawSourcePreviewer.class)
public class SpoolDirSource extends BaseSpoolDirSource {

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      description = "The data format in the files",
      defaultValue = "",
      displayPosition = 100)
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = FileDataTypeChooserValues.class)
  public FileDataType fileDataType;

  // CSV Configuration

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      label = "File Type",
      description = "The specific Delimited File format",
      defaultValue = "CSV",
      displayPosition = 200,
      group = "DELIMITED_FILES",
      dependsOn = "fileDataType",
      triggeredByValue = "DELIMITED_FILES")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = CvsFileModeChooserValues.class)
  public CsvFileMode csvFileFormat;

  @ConfigDef(required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Header Line",
      description = "If the files start with a header line",
      defaultValue = "TRUE",
      displayPosition = 210,
      group = "DELIMITED_FILES",
      dependsOn = "fileDataType",
      triggeredByValue = "DELIMITED_FILES")
  public boolean hasHeaderLine;

  @ConfigDef(required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Convert to Map",
      description = "Converts delimited values to a map based on the header or placeholder header values",
      defaultValue = "TRUE",
      displayPosition = 220,
      group = "DELIMITED_FILES",
      dependsOn = "fileDataType",
      triggeredByValue = "DELIMITED_FILES")
  public boolean convertToMap;

  // JSON Configuration

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      label = "Content",
      description = "Indicates if the JSON files have a single JSON array object or multiple JSON objects",
      defaultValue = "ARRAY_OBJECTS",
      displayPosition = 300,
      group = "JSON_FILES",
      dependsOn = "fileDataType",
      triggeredByValue = "JSON_FILES")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = JsonFileModeChooserValues.class)
  public JsonFileMode jsonContent;

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      label = "Maximum Object Length",
      description = "Larger objects are not processed",
      defaultValue = "4096",
      displayPosition = 310,
      group = "JSON_FILES",
      dependsOn = "fileDataType",
      triggeredByValue = "JSON_FILES")
  public int maxJsonObjectLen;

  // LOG Configuration

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      label = "Maximum Line Length",
      description = "Longer lines are truncated",
      defaultValue = "1024",
      displayPosition = 400,
      group = "LOG_FILES",
      dependsOn = "fileDataType",
      triggeredByValue = "LOG_FILES")
  public int maxLogLineLength;

  // XML Configuration

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      label = "Element Record Delimiter",
      description = "XML element name that acts as record delimiter",
      defaultValue = "record",
      displayPosition = 500,
      group = "XML_FILES",
      dependsOn = "fileDataType",
      triggeredByValue = "XML_FILES")
  public String xmlRecordElement;

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      label = "Maximum Record Length",
      description = "Larger records are not processed",
      defaultValue = "4096",
      displayPosition = 510,
      group = "XML_FILES",
      dependsOn = "fileDataType",
      triggeredByValue = "XML_FILES")
  public int maxXmlObjectLen;

  private DataProducer dataProducer;

  @Override
  protected void init() throws StageException {
    super.init();
    switch (fileDataType) {
      case LOG_FILES:
        dataProducer = new LogDataProducer(getContext(), maxLogLineLength);
        break;
      case JSON_FILES:
        dataProducer = new JsonDataProducer(getContext(), jsonContent, maxJsonObjectLen);
        break;
      case DELIMITED_FILES:
        dataProducer = new CsvDataProducer(getContext(), csvFileFormat, hasHeaderLine, convertToMap);
        break;
      case XML_FILES:
        dataProducer = new XmlDataProducer(getContext(), xmlRecordElement, maxXmlObjectLen);
        break;
    }
  }

  @Override
  public long produce(File file, long offset, int maxBatchSize, BatchMaker batchMaker) throws StageException,
      BadSpoolFileException {
    return dataProducer.produce(file, offset, maxBatchSize, batchMaker);
  }

}
