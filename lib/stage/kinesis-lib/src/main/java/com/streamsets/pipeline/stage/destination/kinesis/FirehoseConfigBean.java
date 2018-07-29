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
package com.streamsets.pipeline.stage.destination.kinesis;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.stage.common.DataFormatGroups;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.pipeline.stage.lib.aws.AWSRegions;
import com.streamsets.pipeline.stage.lib.kinesis.Errors;
import com.streamsets.pipeline.stage.lib.kinesis.KinesisConfigBean;

import java.util.List;

import static com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil.KINESIS_CONFIG_BEAN;

public class FirehoseConfigBean extends KinesisConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "EXISTING",
      label = "Destination Type",
      description = "To auto-create a new S3 or Redshift stream select the appropriate value. Otherwise choose " +
          "the default Existing Stream.",
      displayPosition = 10,
      group = "#0"
  )
  @ValueChooserModel(FirehoseDestinationChooserValues.class)
  public FirehoseDestinationType destinationType;

  @ConfigDefBean(groups = {"DATA_FORMAT"})
  public DataGeneratorFormatConfig dataFormatConfig;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JSON",
      label = "Data Format",
      description = "Data format to use when receiving records from Kinesis",
      displayPosition = 1,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(FirehoseDataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Maximum Record Size (KB)",
      description = "Records larger than this will be sent to the error pipeline.",
      displayPosition = 30,
      group = "#0"
  )
  public int maxRecordSize = 1000;

  public void init(
      Stage.Context context,
      List<Stage.ConfigIssue> issues
  ) {
    dataFormatConfig.init(
        context,
        dataFormat,
        Groups.KINESIS.name(),
        KINESIS_CONFIG_BEAN + ".dataFormatConfig",
        issues
    );

    if (dataFormat == DataFormat.JSON && dataFormatConfig.jsonMode == JsonMode.ARRAY_OBJECTS) {
      issues.add(
          context.createConfigIssue(
              DataFormatGroups.DATA_FORMAT.name(),
              KINESIS_CONFIG_BEAN + ".dataFormatConfig.jsonMode",
              Errors.KINESIS_07
          )
      );
    }

    if (region == AWSRegions.OTHER && (endpoint == null || endpoint.isEmpty())) {
      issues.add(
          context.createConfigIssue(
              Groups.KINESIS.name(),
              KINESIS_CONFIG_BEAN + ".endpoint",
              Errors.KINESIS_09
          )
      );
    }
  }
}
