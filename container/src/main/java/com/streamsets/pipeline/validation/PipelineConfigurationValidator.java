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
package com.streamsets.pipeline.validation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.ModelType;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.container.TextUtils;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PipelineConfigurationValidator {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineConfigurationValidator.class);

  private final StageLibraryTask stageLibrary;
  private final String name;
  private final PipelineConfiguration pipelineConfiguration;
  private final Issues issues;
  private final List<String> openLanes;
  private boolean validated;
  private boolean canPreview;

  public PipelineConfigurationValidator(StageLibraryTask stageLibrary, String name, PipelineConfiguration pipelineConfiguration) {
    Preconditions.checkNotNull(stageLibrary, "stageLibrary cannot be null");
    Preconditions.checkNotNull(name, "name cannot be null");
    Preconditions.checkNotNull(pipelineConfiguration, "pipelineConfiguration cannot be null");
    this.stageLibrary = stageLibrary;
    this.name = name;
    this.pipelineConfiguration = pipelineConfiguration;
    issues = new Issues();
    openLanes = new ArrayList<>();
  }

  boolean sortStages() {
    boolean ok = true;
    List<StageConfiguration> original = new ArrayList<>(pipelineConfiguration.getStages());
    List<StageConfiguration> sorted = new ArrayList<>();
    Set<String> producedOutputs = new HashSet<>();
    while (ok && !original.isEmpty()) {
      int prior = sorted.size();
      Iterator<StageConfiguration> it = original.iterator();
      while (it.hasNext()) {
        StageConfiguration stage = it.next();
        if (producedOutputs.containsAll(stage.getInputLanes())) {
          producedOutputs.addAll(stage.getOutputLanes());
          it.remove();
          sorted.add(stage);
        }
      }
      if (prior == sorted.size()) {
        // pipeline has not stages at all
        List<String> names = new ArrayList<String>(original.size());
        for (StageConfiguration stage : original) {
          names.add(stage.getInstanceName());
        }
        issues.addP(new Issue(ValidationErrors.VALIDATION_0002, names));
        ok = false;
      }
    }
    sorted.addAll(original);
    pipelineConfiguration.setStages(sorted);
    return ok;
  }

  public boolean validate() {
    Preconditions.checkState(!validated, "Already validated");
    validated = true;
    LOG.trace("Pipeline '{}' starting validation", name);
    canPreview = sortStages();
    canPreview &= checkIfPipelineIsEmpty();
    canPreview &= validatePipelineConfiguration();
    canPreview &= validatePipelineLanes();
    if (LOG.isTraceEnabled() && issues.hasIssues()) {
      for (Issue issue : issues.getPipelineIssues()) {
        LOG.trace("Pipeline '{}', {}", name, issue);
      }
      for (List<StageIssue> stageIssues : issues.getStageIssues().values()) {
        for (StageIssue stageIssue  :stageIssues) {
          LOG.trace("Pipeline '{}', {}", name, stageIssue);
        }
      }
    }
    LOG.debug("Pipeline '{}' validation. valid={}, canPreview={}, issuesCount={}", name, !issues.hasIssues(),
              canPreview, issues.getIssueCount());
    return !issues.hasIssues();
  }

  public boolean canPreview() {
    Preconditions.checkState(validated, "validate() has not been called");
    return canPreview;
  }

  public Issues getIssues() {
    Preconditions.checkState(validated, "validate() has not been called");
    return issues;
  }

  public List<String> getOpenLanes() {
    Preconditions.checkState(validated, "validate() has not been called");
    return openLanes;
  }

  boolean checkIfPipelineIsEmpty() {
    boolean preview = true;
    if (pipelineConfiguration.getStages().isEmpty()) {
      // pipeline has not stages at all
      issues.addP(new Issue(ValidationErrors.VALIDATION_0001));
      preview = false;
    }
    return preview;
  }

  @VisibleForTesting
  boolean validatePipelineConfiguration() {
    boolean preview = true;
    Set<String> stageNames = new HashSet<String>();
    boolean shouldBeSource = true;
    for (StageConfiguration stageConf : pipelineConfiguration.getStages()) {
      if (stageNames.contains(stageConf.getInstanceName())) {
        // duplicate stage instance name in the pipeline
        issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(), ValidationErrors.VALIDATION_0005));
        preview = false;
      }
      StageDefinition stageDef = stageLibrary.getStage(stageConf.getLibrary(), stageConf.getStageName(),
                                                       stageConf.getStageVersion());
      if (stageDef == null) {
        // stage configuration refers to an undefined stage definition
        issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(), ValidationErrors.VALIDATION_0006,
                                  stageConf.getLibrary(), stageConf.getStageName(), stageConf.getStageVersion()));
        preview = false;
      } else {
        if (shouldBeSource) {
          if (stageDef.getType() != StageType.SOURCE) {
            // first stage must be a Source
            issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(), ValidationErrors.VALIDATION_0003));
            preview = false;
          }
        } else {
          if (stageDef.getType() == StageType.SOURCE) {
            // no stage other than first stage can be a Source
            issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(), ValidationErrors.VALIDATION_0004));
            preview = false;
          }
        }
        shouldBeSource = false;
        if (!stageConf.isSystemGenerated() && !TextUtils.isValidName(stageConf.getInstanceName())) {
          // stage instance name has an invalid name (it must match '[0-9A-Za-z_]+')
          issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(), ValidationErrors.VALIDATION_0016,
                                                 TextUtils.VALID_NAME));
          preview = false;
        }
        for (String lane : stageConf.getInputLanes()) {
          if (!TextUtils.isValidName(lane)) {
            // stage instance input lane has an invalid name (it must match '[0-9A-Za-z_]+')
            issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(), ValidationErrors.VALIDATION_0017, lane,
                                                   TextUtils.VALID_NAME));
            preview = false;
          }
        }
        for (String lane : stageConf.getOutputLanes()) {
          if (!TextUtils.isValidName(lane)) {
            // stage instance output lane has an invalid name (it must match '[0-9A-Za-z_]+')
            issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(), ValidationErrors.VALIDATION_0018, lane,
                                                   TextUtils.VALID_NAME));
            preview = false;
          }
        }
        switch (stageDef.getType()) {
          case SOURCE:
            if (!stageConf.getInputLanes().isEmpty()) {
              // source stage cannot have input lanes
              issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(), ValidationErrors.VALIDATION_0012,
                                                     stageDef.getType(), stageConf.getInputLanes()));
              preview = false;
            }
            if (stageConf.getOutputLanes().isEmpty()) {
              // source stage must have at least one output lane
              issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(), ValidationErrors.VALIDATION_0015,
                                                     stageDef.getType()));
            }
            break;
          case PROCESSOR:
            if (stageConf.getInputLanes().isEmpty()) {
              // processor stage must have at least one input lane
              issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(), ValidationErrors.VALIDATION_0014,
                                                     stageDef.getType()));
              preview = false;
            }
            if (stageConf.getOutputLanes().isEmpty()) {
              // processor stage must have at least one ouput lane
              issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(), ValidationErrors.VALIDATION_0015,
                                                     stageDef.getType()));
            }
            break;
          case TARGET:
            if (stageConf.getInputLanes().isEmpty()) {
              // target stage must have at least one input lane
              issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(), ValidationErrors.VALIDATION_0014,
                                                     stageDef.getType()));
              preview = false;
            }
            if (!stageConf.getOutputLanes().isEmpty()) {
              // target stage cannot have output lanes
              issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(), ValidationErrors.VALIDATION_0013,
                                        stageDef.getType(), stageConf.getOutputLanes()));
              preview = false;
            }
            break;
        }
        for (ConfigDefinition confDef : stageDef.getConfigDefinitions()) {
          if (stageConf.getConfig(confDef.getName()) == null && confDef.isRequired()) {
            // stage configuration does not have a configuration that is required
            issues.add(StageIssue.createConfigIssue(stageConf.getInstanceName(), confDef.getName(),
                                                    ValidationErrors.VALIDATION_0007));
            preview = false;
          }
        }
        for (ConfigConfiguration conf : stageConf.getConfiguration()) {
          ConfigDefinition confDef = stageDef.getConfigDefinition(conf.getName());
          if (confDef == null) {
            // stage configuration defines an invalid configuration
            issues.add(StageIssue.createConfigIssue(stageConf.getInstanceName(), conf.getName(),
                                                    ValidationErrors.VALIDATION_0008));
          } else if (conf.getValue() == null && confDef.isRequired()) {
            // stage configuration has a NULL value for a configuration that requires a value
            issues.add(StageIssue.createConfigIssue(stageConf.getInstanceName(), confDef.getName(),
                                                    ValidationErrors.VALIDATION_0007));
            preview = false;
          }
          if (conf.getValue() != null) {
            switch (confDef.getType()) {
              case BOOLEAN:
                if (!(conf.getValue() instanceof Boolean)) {
                  // stage configuration must be a boolean
                  issues.add(StageIssue.createConfigIssue(stageConf.getInstanceName(), confDef.getName(),
                                                          ValidationErrors.VALIDATION_0009, confDef.getType()));
                  preview = false;
                }
                break;
              case INTEGER:
                if (!(conf.getValue() instanceof Long || conf.getValue() instanceof Integer)) {
                  // stage configuration must be a number
                  issues.add(StageIssue.createConfigIssue(stageConf.getInstanceName(), confDef.getName(),
                                                          ValidationErrors.VALIDATION_0009, confDef.getType()));
                  preview = false;
                }
                break;
              case STRING:
                //NOP
                break;
              case MODEL:
                if(confDef.getModel().getModelType() == ModelType.VALUE_CHOOSER) {
                  if(!(conf.getValue() instanceof String)) {
                    // stage configuration must be a model
                    issues.add(StageIssue.createConfigIssue(stageConf.getInstanceName(), confDef.getName(),
                                                            ValidationErrors.VALIDATION_0009, "String"));
                  }
                } else if (!(conf.getValue() instanceof Map || conf.getValue() instanceof List)) {
                  //TODO introduce schema for models so we can validate
                  // stage configuration must be a model
                  issues.add(StageIssue.createConfigIssue(stageConf.getInstanceName(), confDef.getName(),
                                                          ValidationErrors.VALIDATION_0009, confDef.getType()));
                  preview = false;
                }
                break;
            }
          }
        }
      }
      stageNames.add(stageConf.getInstanceName());
    }
    return preview;
  }

  @VisibleForTesting
  boolean validatePipelineLanes() {
    boolean preview = true;
    List<StageConfiguration> stagesConf = pipelineConfiguration.getStages();
    for (int i = 0; i < stagesConf.size(); i++) {
      StageConfiguration stageConf = stagesConf.get(i);
      Set<String> openOutputs = new HashSet<String>(stageConf.getOutputLanes());
      for (int j = i + 1; j < stagesConf.size(); j++) {
        StageConfiguration downStreamStageConf = stagesConf.get(j);
        Set<String> duplicateOutputs = Sets.intersection(new HashSet<String>(stageConf.getOutputLanes()),
                                                         new HashSet<String>(downStreamStageConf.getOutputLanes()));
        if (!duplicateOutputs.isEmpty()) {
          // there is more than one stage defining the same output lane
          issues.add(StageIssue.createStageIssue(downStreamStageConf.getInstanceName(),
                                                 ValidationErrors.VALIDATION_0010,
                                                 duplicateOutputs, stageConf.getInstanceName()));
          preview = false;
        }

        openOutputs.removeAll(downStreamStageConf.getInputLanes());
      }
      if (!openOutputs.isEmpty()) {
        openLanes.addAll(openOutputs);
        // the stage has open output lanes
        issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(), ValidationErrors.VALIDATION_0011,
                                               openOutputs));
      }
    }
    return preview;
  }

}
