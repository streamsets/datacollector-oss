/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.devtest.rawdata;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DSource;
import com.streamsets.pipeline.stage.origin.lib.DataFormatConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@GenerateResourceBundle
@StageDef(version = 1,
  label = "Dev Raw Data Source",
  description = "Add Raw data to the source.",
  execution = ExecutionMode.STANDALONE,
  icon = "dev.png")
@ConfigGroups(value = RawDataSourceGroups.class)
public class RawDataDSource extends DSource {
  private static final Logger LOG = LoggerFactory.getLogger(RawDataDSource.class);

  @ConfigDefBean(groups = "RAW")
  public DataFormatConfig dataFormatConfig;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.TEXT,
    mode = ConfigDef.Mode.JSON,
    label = "Raw Data",
    evaluation = ConfigDef.Evaluation.IMPLICIT,
    displayPosition = 20,
    group = "RAW"
  )
  public String rawData;


  @Override
  protected Source createSource() {
    return new RawDataSource(dataFormatConfig, rawData);
  }
}
