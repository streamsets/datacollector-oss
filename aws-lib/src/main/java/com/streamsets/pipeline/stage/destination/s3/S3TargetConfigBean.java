/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.s3;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.CharsetChooserValues;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvModeChooserValues;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.JsonModeChooserValues;
import com.streamsets.pipeline.lib.el.StringEL;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.avro.AvroDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.binary.BinaryDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.delimited.DelimitedDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.text.TextDataGeneratorFactory;
import com.streamsets.pipeline.stage.origin.s3.S3AdvancedConfig;
import com.streamsets.pipeline.stage.origin.s3.S3Config;

import java.nio.charset.Charset;
import java.util.List;

public class S3TargetConfigBean {

  @ConfigDefBean(groups = {"S3"})
  public S3Config s3Config;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "sdc",
    description = "Prefix for file names that will be uploaded on Amazon S3",
    label = "File Name Prefix",
    displayPosition = 190,
    group = "S3"
  )
  public String fileNamePrefix;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    label = "Data Format",
    displayPosition = 200,
    group = "S3"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "UTF-8",
    label = "Data  Charset",
    displayPosition = 210,
    group = "S3",
    dependsOn = "dataFormat",
    triggeredByValue = {"TEXT", "JSON", "DELIMITED", "XML", "LOG"}
  )
  @ValueChooserModel(CharsetChooserValues.class)
  public String charset;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Compress with gzip",
    displayPosition = 220,
    group = "S3"
  )
  public boolean compress;

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
  @ValueChooserModel(CsvModeChooserValues.class)
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
  @ValueChooserModel(S3CsvHeaderChooserValues.class)
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
  @ValueChooserModel(JsonModeChooserValues.class)
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

  /********  For Binary Content  ***********/

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "/",
    label = "Binary Field Path",
    description = "Field to write data to Kafka",
    displayPosition = 330,
    group = "BINARY",
    dependsOn = "dataFormat",
    triggeredByValue = "BINARY",
    elDefs = {StringEL.class}
  )
  @FieldSelectorModel(singleValued = true)
  public String binaryFieldPath;

  @ConfigDefBean(groups = {"ADVANCED"})
  public S3AdvancedConfig advancedConfig;

  public List<Stage.ConfigIssue> init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    s3Config.init(context, issues, advancedConfig);

    if(s3Config.bucket == null || s3Config.bucket.isEmpty()) {
      issues.add(context.createConfigIssue(Groups.S3.name(), "bucket", Errors.S3_01));
    } else if (!s3Config.getS3Client().doesBucketExist(s3Config.bucket)) {
      issues.add(context.createConfigIssue(Groups.S3.name(), "bucket", Errors.S3_02, s3Config.bucket));
    }

    validateDataFormatAndSpecificConfig(dataFormat, context, Groups.S3.name(), "dataFormat", issues);

    if(issues.size() == 0) {
      generatorFactory = createDataGeneratorFactory(context);
    }
    return issues;
  }

  public void destroy() {
    s3Config.destroy();
  }

  private void validateDataFormatAndSpecificConfig(
    DataFormat dataFormat,
    Stage.Context context,
    String groupName,
    String configName,
    List<Stage.ConfigIssue> issues
  ) {
    switch (dataFormat) {
      case TEXT:
        //required field configuration to be set and it is "/" by default
        if(textFieldPath == null || textFieldPath.isEmpty()) {
          issues.add(context.createConfigIssue(Groups.TEXT.name(), "fieldPath", Errors.S3_31));
        }
        break;
      case BINARY:
        //required field configuration to be set and it is "/" by default
        if(binaryFieldPath == null || binaryFieldPath.isEmpty()) {
          issues.add(context.createConfigIssue(Groups.BINARY.name(), "fieldPath", Errors.S3_31));
        }
        break;
      case JSON:
      case DELIMITED:
      case SDC_JSON:
      case AVRO:
        //no-op
        break;
      default:
        issues.add(context.createConfigIssue(groupName, configName, Errors.S3_30, dataFormat));
        //XML is not supported for KafkaTarget
    }
  }

  private DataGeneratorFactory createDataGeneratorFactory(Stage.Context context) {
    DataGeneratorFactoryBuilder builder = new DataGeneratorFactoryBuilder(context,
      dataFormat.getGeneratorFormat());
    if(charset == null || charset.trim().isEmpty()) {
      charset = "UTF-8";
    }
    builder.setCharset(Charset.forName(charset));
    switch (dataFormat) {
      case SDC_JSON:
        break;
      case DELIMITED:
        builder.setMode(csvFileFormat);
        builder.setMode(csvHeader);
        builder.setConfig(DelimitedDataGeneratorFactory.REPLACE_NEWLINES_KEY, csvReplaceNewLines);
        break;
      case TEXT:
        builder.setConfig(TextDataGeneratorFactory.FIELD_PATH_KEY, textFieldPath);
        builder.setConfig(TextDataGeneratorFactory.EMPTY_LINE_IF_NULL_KEY, textEmptyLineIfNull);
        break;
      case JSON:
        builder.setMode(jsonMode);
        break;
      case AVRO:
        builder.setConfig(AvroDataGeneratorFactory.SCHEMA_KEY, avroSchema);
        break;
      case BINARY:
        builder.setConfig(BinaryDataGeneratorFactory.FIELD_PATH_KEY, binaryFieldPath);
        break;
    }
    return builder.build();
  }

  private DataGeneratorFactory generatorFactory;

  public DataGeneratorFactory getGeneratorFactory() {
    return generatorFactory;
  }
}
