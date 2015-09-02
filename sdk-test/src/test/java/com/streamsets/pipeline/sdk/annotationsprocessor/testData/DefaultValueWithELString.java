/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.sdk.annotationsprocessor.testData;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
@GenerateResourceBundle
@StageDef(description = "ELString As Default Value", label = "twitter_source"
  , version = 1)
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
