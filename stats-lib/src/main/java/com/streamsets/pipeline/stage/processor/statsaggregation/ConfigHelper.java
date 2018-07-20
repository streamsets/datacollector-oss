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
package com.streamsets.pipeline.stage.processor.statsaggregation;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.RuleDefinitionsJson;
import com.streamsets.pipeline.api.Stage;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.util.List;

public class ConfigHelper {

  public static RuleDefinitionsJson readRulesDefinition(
      String ruleDefinitionsJson,
      Stage.Context context,
      List<Stage.ConfigIssue> issues) {
    RuleDefinitionsJson ruleDefJson = null;
    if (ruleDefJson != null) {
      try {
        ruleDefJson = ObjectMapperFactory.get().readValue(
          new String(Base64.decodeBase64(ruleDefinitionsJson)),
          RuleDefinitionsJson.class
        );
      } catch (IOException ex) {
        issues.add(
            context.createConfigIssue(
              Groups.STATS.getLabel(),
              "ruleDefinitionsJson",
              Errors.STATS_01,
              ex.getMessage(),
              ex
          )
        );
      }
    }
    return  ruleDefJson;
  }

  public static PipelineConfiguration readPipelineConfig(
      String pipelineConfigJson,
      Stage.Context context,
      List<Stage.ConfigIssue> issues
  ) {
    PipelineConfiguration pipelineConfiguration = null;
    try {
      PipelineConfigurationJson pipelineConfigurationJson = ObjectMapperFactory.get().readValue(
        new String(Base64.decodeBase64(pipelineConfigJson)),
        PipelineConfigurationJson.class
      );
      pipelineConfiguration = BeanHelper.unwrapPipelineConfiguration(pipelineConfigurationJson);
    } catch (IOException ex) {
      issues.add(
          context.createConfigIssue(
          Groups.STATS.getLabel(),
          "pipelineConfigJson",
          Errors.STATS_00,
          ex.getMessage(),
          ex
        )
      );
    }
    return pipelineConfiguration;
  }

}
