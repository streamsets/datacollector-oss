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
package com.streamsets.pipeline.config;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.stagelibrary.StageLibrary;
import com.streamsets.pipeline.util.Issue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PipelineConfigurationValidator {
  private final StageLibrary stageLibrary;
  private final PipelineConfiguration pipelineConfiguration;
  private final List<Issue> issues;
  private boolean validated;

  public PipelineConfigurationValidator(StageLibrary stageLibrary, PipelineConfiguration pipelineConfiguration) {
    Preconditions.checkNotNull(stageLibrary, "stageLibrary cannot be null");
    Preconditions.checkNotNull(pipelineConfiguration, "pipelineConfiguration cannot be null");
    this.stageLibrary = stageLibrary;
    this.pipelineConfiguration = pipelineConfiguration;
    issues = new ArrayList<Issue>();
  }

  public boolean validate() {
    validated = true;
    if (pipelineConfiguration.getStages().isEmpty()) {
      issues.add(new Issue("pipeline", "validation.pipeline.is.empty", "The pipeline is empty"));
    }
    validatePipelineConfiguration();
    validatePipelineLanes();
    return issues.isEmpty();
  }

  public List<Issue>  getIssues() {
    Preconditions.checkState(validated, String.format("validate() has not been called"));
    return ImmutableList.copyOf(issues);
  }

  @VisibleForTesting
  void validatePipelineConfiguration() {
    Set<String> stageNames = new HashSet<String>();
    for (StageConfiguration stage : pipelineConfiguration.getStages()) {
      if (stageNames.contains(stage.getInstanceName())) {
        issues.add(new Issue(stage.getInstanceName(),
                             "validation.instance.already.defined", "Instance '%s' already defined",
                             stage.getInstanceName()));
      }
      StageDefinition stageDef = stageLibrary.getStage(stage.getLibrary(), stage.getStageName(),
                                                       stage.getStageVersion());
      if (stageDef == null) {
        issues.add(new Issue(stage.getInstanceName(),
            "validation.stage.does.not.exist", "Instance '%s', stage does not exist library '%s' name '%s' version '%s'",
            stage.getInstanceName(), stage.getLibrary(), stage.getStageName(), stage.getStageVersion()));
      } else {
        for (ConfigDefinition confDef : stageDef.getConfigDefinitions()) {
          if (stage.getConfig(confDef.getName()) == null && confDef.isRequired()) {
            issues.add(new Issue(stage.getInstanceName(),
                "validation.stage.missing.configuration", "Instance '%s', stage requires configuration for '%s'",
                stage.getInstanceName(), confDef.getName()));
          }
        }
        for (ConfigConfiguration conf : stage.getConfiguration()) {
          ConfigDefinition def = stageDef.getConfigDefinition(conf.getName());
          if (conf.getValue() == null && def.isRequired()) {
            issues.add(new Issue(stage.getInstanceName(),
                "validation.stage.configuration.missing.value", "Instance '%s', stage requires a configuration value for '%s'",
                stage.getInstanceName(), def.getName()));
          }
          if (conf.getValue() != null) {
            switch (def.getType()) {
              case BOOLEAN:
                if (!(conf.getValue() instanceof Boolean)) {
                  issues.add(new Issue(stage.getInstanceName(),
                      "validation.stage.configuration.invalidType", "Instance '%s', configuration '%s' should be a '%s'",
                      stage.getInstanceName(), def.getName(), def.getType()));
                }
                break;
              case INTEGER:
              case LONG:
                if (!(conf.getValue() instanceof Long || conf.getValue() instanceof Integer)) {
                  issues.add(new Issue(stage.getInstanceName(),
                      "validation.stage.configuration.invalidType", "Instance '%s', configuration '%s' should be a '%s'",
                      stage.getInstanceName(), def.getName(), def.getType()));
                }
                break;
              case STRING:
                //NOP
                break;
              case MODEL:
                if (!(conf.getValue() instanceof Map || conf.getValue() instanceof List)) {
                  issues.add(new Issue(stage.getInstanceName(),
                      "validation.stage.configuration.invalidType", "Instance '%s', configuration '%s' should be a '%s'",
                      stage.getInstanceName(), def.getName(), def.getType()));
                }
                break;
            }
          }
        }
      }
      stageNames.add(stage.getInstanceName());
    }
  }

  @VisibleForTesting
  void validatePipelineLanes() {
    Set<String> output = new HashSet<String>();
    Set<String> input = new HashSet<String>();
    for (StageConfiguration stage : pipelineConfiguration.getStages()) {
      output.addAll(stage.getOutputLanes());
      input.addAll(stage.getInputLanes());
    }
    Set<String> open = new HashSet<String>(output);
    open.removeAll(input);
    if (!open.isEmpty()) {
      for (String lane : open) {
        for (StageConfiguration stage : pipelineConfiguration.getStages()) {
          if (stage.getOutputLanes().contains(lane)) {
            issues.add(new Issue(stage.getInstanceName(),
                                 "validation.instance.open.output.lane", "Instance '%s' has an open lane '%s'",
                                 stage.getInstanceName(), lane));
          }
        }
      }
    }
  }

}
