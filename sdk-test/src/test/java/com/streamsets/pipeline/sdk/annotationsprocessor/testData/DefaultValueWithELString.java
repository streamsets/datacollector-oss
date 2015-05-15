/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.annotationsprocessor.testData;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
@GenerateResourceBundle
@StageDef(description = "ELString As Default Value", label = "twitter_source"
  , version = "1.0")
public class DefaultValueWithELString extends BaseSource {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "${10 * SECONDS}",
    label = "Query Interval",
    displayPosition = 60,
    elDefs = {},
    evaluation = ConfigDef.Evaluation.IMPLICIT
  )
  public int intConfig;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "${10 * SECONDS}",
    label = "Query Interval",
    displayPosition = 60,
    elDefs = {},
    evaluation = ConfigDef.Evaluation.IMPLICIT
  )
  public int longConfig;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "${10 * SECONDS}",
    label = "Query Interval",
    displayPosition = 60,
    elDefs = {},
    evaluation = ConfigDef.Evaluation.IMPLICIT
  )
  public boolean booleanConfig;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.CHARACTER,
    defaultValue = "${10 * SECONDS}",
    label = "Query Interval",
    displayPosition = 60,
    elDefs = {},
    evaluation = ConfigDef.Evaluation.IMPLICIT
  )
  public char charConfig;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "${10 * SECONDS}",
    label = "Query Interval",
    displayPosition = 60,
    elDefs = {},
    evaluation = ConfigDef.Evaluation.IMPLICIT
  )
  public long queryInterval;

  public DefaultValueWithELString() {
  }

    @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    return null;
  }

}
