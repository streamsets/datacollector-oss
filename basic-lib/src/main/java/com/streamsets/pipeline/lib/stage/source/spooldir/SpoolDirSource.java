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
    label = "Files in Directory",
    description = "Consumes files from a spool directory",
    icon="files.png")
@ConfigGroups(FileDataType.class)
@RawSource(rawSourcePreviewer = FileRawSourcePreviewer.class, mimeType = "application/json")
public class SpoolDirSource extends BaseSpoolDirSource {

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      label = "File Data Format",
      description = "The data format in the files",
      defaultValue = "",
      displayPosition = 100)
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = FileDataTypeChooserValues.class)
  public FileDataType fileDataType;

  // CSV Configuration

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      label = "CSV Format",
      description = "The specific CSV format of the files",
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
      description = "Indicates if the CSV files start with a header line",
      defaultValue = "TRUE",
      displayPosition = 201,
      group = "DELIMITED_FILES",
      dependsOn = "fileDataType",
      triggeredByValue = "DELIMITED_FILES")
  public boolean hasHeaderLine;

  @ConfigDef(required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Convert to Map",
      description = "Converts CVS data array to a Map using the headers as keys",
      defaultValue = "TRUE",
      displayPosition = 202,
      group = "DELIMITED_FILES",
      dependsOn = "fileDataType",
      triggeredByValue = "DELIMITED_FILES")
  public boolean convertToMap;

  // JSON Configuration

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      label = "JSON Content",
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
      label = "Maximum JSON Object Length",
      description = "The maximum length for a JSON Object being converted to a record, if greater the full JSON " +
                    "object is discarded and processing continues with the next JSON object",
      defaultValue = "4096",
      displayPosition = 301,
      group = "JSON_FILES",
      dependsOn = "fileDataType",
      triggeredByValue = "JSON_FILES")
  public int maxJsonObjectLen;

  // LOG Configuration

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      label = "Maximum Log Line Length",
      description = "The maximum length for log lines, if a line exceeds this length, it will be truncated",
      defaultValue = "1024",
      displayPosition = 400,
      group = "LOG_FILES",
      dependsOn = "fileDataType",
      triggeredByValue = "LOG_FILES")
  public int maxLogLineLength;

  // XML Configuration

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      label = "Maximum XML Record Length",
      description = "The maximum length for an XML record, if a record exceeds this length, it will be discarded",
      defaultValue = "4096",
      displayPosition = 500,
      group = "XML_FILES",
      dependsOn = "fileDataType",
      triggeredByValue = "XML_FILES")
  public int maxXmlObjectLen;

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      label = "XML Element Record Delimiter",
      description = "The first level XML element name that acts as record delimiter",
      defaultValue = "",
      displayPosition = 501,
      group = "XML_FILES",
      dependsOn = "fileDataType",
      triggeredByValue = "XML_FILES")
  public String xmlRecordElement;

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
