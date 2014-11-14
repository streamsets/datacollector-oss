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
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class PipelineConfigurationValidator {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineConfigurationValidator.class);

  private static final String PIPELINE_IS_EMPTY_KEY = "validation.pipeline.is.empty";
  private static final String PIPELINE_IS_EMPTY_DEFAULT = "The pipeline is empty";
  private static final String STAGES_ARE_NOT_FULLY_WIRED_KEY = "validation.pipeline.stages.not.fully.wired";
  private static final String STAGES_ARE_NOT_FULLY_WIRED_DEFAULT =
      "Stages are not fully wired, cannot reach the following stages '{}'";
  private static final String FIRST_STAGE_MUST_BE_A_SOURCE_KEY = "validation.first.stage.must.be.source";
  private static final String FIRST_STAGE_MUST_BE_A_SOURCE_DEFAULT = "The first stage must be a Source";
  private static final String STAGE_CANNOT_BE_SOURCE_KEY = "validation.stage.cannot.be.source";
  private static final String STAGE_CANNOT_BE_SOURCE_DEFAULT = "This stage cannot be a Source";
  private static final String INSTANCE_ALREADY_DEFINED_KEY = "validation.instance.already.defined";
  private static final String INSTANCE_ALREADY_DEFINED_DEFAULT = "Instance name already defined";
  private static final String STAGE_DOES_NOT_EXIST_KEY = "validation.stage.does.not.exist";
  private static final String STAGE_DOES_NOT_EXIST_DEFAULT =
      "Stage definition does not exist, library '{}' name '{}' version '{}'";
  private static final String STAGE_MISSING_CONFIGURATION_KEY = "validation.stage.missing.configuration";
  private static final String STAGE_MISSING_CONFIGURATION_DEFAULT = "Configuration value is required";
  private static final String STAGE_INVALID_CONFIGURATION_KEY = "validation.stage.invalid.configuration";
  private static final String STAGE_INVALID_CONFIGURATION_DEFAULT = "Invalid configuration";
  private static final String STAGE_CONFIGURATION_INVALID_TYPE_KEY = "validation.stage.configuration.invalidType";
  private static final String STAGE_CONFIGURATION_INVALID_TYPE_DEFAULT = "Configuration should be a '{}'";
  private static final String STAGE_OUTPUT_LANES_ALREADY_EXISTS_KEY = "validation.stage.output.lane.already.exists";
  private static final String STAGE_OUTPUT_LANES_ALREADY_EXISTS_DEFAULT =
      "Output lanes '{}' already defined by stage instance '{}'";
  private static final String INSTANCE_OPEN_OUTPUT_LANES_KEY = "validation.instance.open.output.lanes";
  private static final String INSTANCE_OPEN_OUTPUT_LANES_DEFAULT = "Instance has open lanes '{}'";
  private static final String STAGE_CANNOT_HAVE_INPUT_KEY = "validation.stage.cannot.have.input.lanes";
  private static final String STAGE_CANNOT_HAVE_INPUT_DEFAULT = "{} cannot have input lanes '{}'";
  private static final String STAGE_CANNOT_HAVE_OUTPUT_KEY = "validation.stage.cannot.have.output.lanes";
  private static final String STAGE_CANNOT_HAVE_OUTPUT_DEFAULT = "{} cannot have output lanes '{}'";
  private static final String STAGE_MUST_HAVE_INPUT_KEY = "validation.stage.must.have.input.lanes";
  private static final String STAGE_MUST_HAVE_INPUT_DEFAULT = "{} must have input lanes";
  private static final String STAGE_MUST_HAVE_OUTPUT_KEY = "validation.stage.must.have.output.lanes";
  private static final String STAGE_MUST_HAVE_OUTPUT_DEFAULT = "{} must have output lanes";
  private static final String STAGE_INVALID_INSTANCE_NAME_KEY = "validation.invalid.instance.name";
  private static final String STAGE_INVALID_INSTANCE_NAME_DEFAULT =
      "Invalid instance name, names can only contain the following characters '{}'";
  private static final String STAGE_INVALID_INPUT_LANE_NAME_KEY = "validation.invalid.input.lane.name";
  private static final String STAGE_INVALID_INPUT_LANE_DEFAULT =
      "Invalid input lane names '{}', lanes can only contain the following characters '{}'";
  private static final String STAGE_INVALID_OUTPUT_LANE_NAME_KEY = "validation.invalid.output.lane.name";
  private static final String STAGE_INVALID_OUTPUT_LANE_DEFAULT =
      "Invalid output lane names '{}', lanes can only contain the following characters '{}'";

  private static final String VALID_NAME= "[0-9A-Za-z_]";

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
    openLanes = new ArrayList<String>();
  }

  boolean sortStages() {
    boolean ok = true;
    List<StageConfiguration> original = new ArrayList<StageConfiguration>(pipelineConfiguration.getStages());
    List<StageConfiguration> sorted = new ArrayList<StageConfiguration>();
    Set<String> producedOutputs = new HashSet<String>();
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
        issues.addP(new Issue(STAGES_ARE_NOT_FULLY_WIRED_KEY, STAGES_ARE_NOT_FULLY_WIRED_DEFAULT, names));
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

  private static final Pattern VALID_NAME_PATTERN = Pattern.compile(VALID_NAME + "+");

  @VisibleForTesting
  static boolean isValidName(String name) {
    return (name != null) && VALID_NAME_PATTERN.matcher(name).matches();
  }

  boolean checkIfPipelineIsEmpty() {
    boolean preview = true;
    if (pipelineConfiguration.getStages().isEmpty()) {
      // pipeline has not stages at all
      issues.addP(new Issue(PIPELINE_IS_EMPTY_KEY, PIPELINE_IS_EMPTY_DEFAULT));
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
        issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                               INSTANCE_ALREADY_DEFINED_KEY, INSTANCE_ALREADY_DEFINED_DEFAULT));
        preview = false;
      }
      StageDefinition stageDef = stageLibrary.getStage(stageConf.getLibrary(), stageConf.getStageName(),
                                                       stageConf.getStageVersion());
      if (stageDef == null) {
        // stage configuration refers to an undefined stage definition
        issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                  STAGE_DOES_NOT_EXIST_KEY, STAGE_DOES_NOT_EXIST_DEFAULT,
                                  stageConf.getLibrary(), stageConf.getStageName(), stageConf.getStageVersion()));
        preview = false;
      } else {
        if (shouldBeSource) {
          if (stageDef.getType() != StageType.SOURCE) {
            // first stage must be a Source
            issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                      FIRST_STAGE_MUST_BE_A_SOURCE_KEY, FIRST_STAGE_MUST_BE_A_SOURCE_DEFAULT));
            preview = false;
          }
        } else {
          if (stageDef.getType() == StageType.SOURCE) {
            // no stage other than first stage can be a Source
            issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                      STAGE_CANNOT_BE_SOURCE_KEY, STAGE_CANNOT_BE_SOURCE_DEFAULT));
            preview = false;
          }
        }
        shouldBeSource = false;
        if (!stageConf.isSystemGenerated() && !isValidName(stageConf.getInstanceName())) {
          // stage instance name has an invalid name (it must match '[0-9A-Za-z_]+')
          issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                    STAGE_INVALID_INSTANCE_NAME_KEY, STAGE_INVALID_INSTANCE_NAME_DEFAULT, VALID_NAME));
          preview = false;
        }
        for (String lane : stageConf.getInputLanes()) {
          if (!isValidName(lane)) {
            // stage instance input lane has an invalid name (it must match '[0-9A-Za-z_]+')
            issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                      STAGE_INVALID_INPUT_LANE_NAME_KEY, STAGE_INVALID_INPUT_LANE_DEFAULT, lane,
                                      VALID_NAME));
            preview = false;
          }
        }
        for (String lane : stageConf.getOutputLanes()) {
          if (!isValidName(lane)) {
            // stage instance output lane has an invalid name (it must match '[0-9A-Za-z_]+')
            issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                      STAGE_INVALID_OUTPUT_LANE_NAME_KEY, STAGE_INVALID_OUTPUT_LANE_DEFAULT, lane,
                                      VALID_NAME));
            preview = false;
          }
        }
        switch (stageDef.getType()) {
          case SOURCE:
            if (!stageConf.getInputLanes().isEmpty()) {
              // source stage cannot have input lanes
              issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                        STAGE_CANNOT_HAVE_INPUT_KEY, STAGE_CANNOT_HAVE_INPUT_DEFAULT,
                                        stageDef.getType(), stageConf.getInputLanes()));
              preview = false;
            }
            if (stageConf.getOutputLanes().isEmpty()) {
              // source stage must have at least one output lane
              issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                                     STAGE_MUST_HAVE_OUTPUT_KEY, STAGE_MUST_HAVE_OUTPUT_DEFAULT,
                                                     stageDef.getType()));
            }
            break;
          case PROCESSOR:
            if (stageConf.getInputLanes().isEmpty()) {
              // processor stage must have at least one input lane
              issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                        STAGE_MUST_HAVE_INPUT_KEY, STAGE_MUST_HAVE_INPUT_DEFAULT, stageDef.getType()));
              preview = false;
            }
            if (stageConf.getOutputLanes().isEmpty()) {
              // processor stage must have at least one ouput lane
              issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                        STAGE_MUST_HAVE_OUTPUT_KEY, STAGE_MUST_HAVE_OUTPUT_DEFAULT,
                                        stageDef.getType()));
            }
            break;
          case TARGET:
            if (stageConf.getInputLanes().isEmpty()) {
              // target stage must have at least one input lane
              issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                                     STAGE_MUST_HAVE_INPUT_KEY, STAGE_MUST_HAVE_INPUT_DEFAULT,
                                                     stageDef.getType()));
              preview = false;
            }
            if (!stageConf.getOutputLanes().isEmpty()) {
              // target stage cannot have output lanes
              issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                        STAGE_CANNOT_HAVE_OUTPUT_KEY, STAGE_CANNOT_HAVE_OUTPUT_DEFAULT,
                                        stageDef.getType(), stageConf.getOutputLanes()));
              preview = false;
            }
            break;
        }
        for (ConfigDefinition confDef : stageDef.getConfigDefinitions()) {
          if (stageConf.getConfig(confDef.getName()) == null && confDef.isRequired()) {
            // stage configuration does not have a configuration that is required
            issues.add(StageIssue.createConfigIssue(stageConf.getInstanceName(), confDef.getName(),
                                                    STAGE_MISSING_CONFIGURATION_KEY,
                                                    STAGE_MISSING_CONFIGURATION_DEFAULT));
            preview = false;
          }
        }
        for (ConfigConfiguration conf : stageConf.getConfiguration()) {
          ConfigDefinition confDef = stageDef.getConfigDefinition(conf.getName());
          if (confDef == null) {
            // stage configuration defines an invalid configuration
            issues.add(StageIssue.createConfigIssue(stageConf.getInstanceName(), conf.getName(),
                                                    STAGE_INVALID_CONFIGURATION_KEY, STAGE_INVALID_CONFIGURATION_DEFAULT));
          } else if (conf.getValue() == null && confDef.isRequired()) {
            // stage configuration has a NULL value for a configuration that requires a value
            issues.add(StageIssue.createConfigIssue(stageConf.getInstanceName(), confDef.getName(),
                                      STAGE_MISSING_CONFIGURATION_KEY, STAGE_MISSING_CONFIGURATION_DEFAULT));
            preview = false;
          }
          if (conf.getValue() != null) {
            switch (confDef.getType()) {
              case BOOLEAN:
                if (!(conf.getValue() instanceof Boolean)) {
                  // stage configuration must be a boolean
                  issues.add(StageIssue.createConfigIssue(stageConf.getInstanceName(), confDef.getName(),
                                            STAGE_CONFIGURATION_INVALID_TYPE_KEY,
                                            STAGE_CONFIGURATION_INVALID_TYPE_DEFAULT, confDef.getType()));
                  preview = false;
                }
                break;
              case INTEGER:
                if (!(conf.getValue() instanceof Long || conf.getValue() instanceof Integer)) {
                  // stage configuration must be a number
                  issues.add(StageIssue.createConfigIssue(stageConf.getInstanceName(), confDef.getName(),
                                            STAGE_CONFIGURATION_INVALID_TYPE_KEY,
                                            STAGE_CONFIGURATION_INVALID_TYPE_DEFAULT, confDef.getType()));
                  preview = false;
                }
                break;
              case STRING:
                //NOP
                break;
              case MODEL:
                //TODO introduce schema for models so we can validate
                if (!(conf.getValue() instanceof Map || conf.getValue() instanceof List)) {
                  // stage configuration must be a model
                  issues.add(StageIssue.createConfigIssue(stageConf.getInstanceName(), confDef.getName(),
                                            STAGE_CONFIGURATION_INVALID_TYPE_KEY,
                                            STAGE_CONFIGURATION_INVALID_TYPE_DEFAULT, confDef.getType()));
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
                                                 STAGE_OUTPUT_LANES_ALREADY_EXISTS_KEY,
                                                 STAGE_OUTPUT_LANES_ALREADY_EXISTS_DEFAULT,
                                                 duplicateOutputs, stageConf.getInstanceName()));
          preview = false;
        }

        openOutputs.removeAll(downStreamStageConf.getInputLanes());
      }
      if (!openOutputs.isEmpty()) {
        openLanes.addAll(openOutputs);
        // the stage has open output lanes
        issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(), INSTANCE_OPEN_OUTPUT_LANES_KEY,
                                               INSTANCE_OPEN_OUTPUT_LANES_DEFAULT, openOutputs));
      }
    }
    return preview;
  }

}
