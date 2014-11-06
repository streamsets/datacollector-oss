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
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.stagelibrary.StageLibrary;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class PipelineConfigurationValidator {

  private static final String PIPELINE_IS_EMPTY_KEY = "validation.pipeline.is.empty";
  private static final String PIPELINE_IS_EMPTY_DEFAULT = "The pipeline is empty";
  private static final String FIRST_STAGE_MUST_BE_A_SOURCE_KEY = "validation.first.stage.must.be.source";
  private static final String FIRST_STAGE_MUST_BE_A_SOURCE_DEFAULT = "The first stage must be a Source";
  private static final String STAGE_CANNOT_BE_SOURCE_KEY = "validation.stage.cannot.be.source";
  private static final String STAGE_CANNOT_BE_SOURCE_DEFAULT = "This stage cannot be a Source";
  private static final String INSTANCE_ALREADY_DEFINED_KEY = "validation.instance.already.defined";
  private static final String INSTANCE_ALREADY_DEFINED_DEFAULT = "Instance name already defined";
  private static final String STAGE_DOES_NOT_EXIST_KEY = "validation.stage.does.not.exist";
  private static final String STAGE_DOES_NOT_EXIST_DEFAULT =
      "Stage definition does not exist, library '%s' name '%s' version '%s'";
  private static final String STAGE_MISSING_CONFIGURATION_KEY = "validation.stage.missing.configuration";
  private static final String STAGE_MISSING_CONFIGURATION_DEFAULT = "Configuration value is required";
  private static final String STAGE_CONFIGURATION_INVALID_TYPE_KEY = "validation.stage.configuration.invalidType";
  private static final String STAGE_CONFIGURATION_INVALID_TYPE_DEFAULT = "Configuration should be a '%s'";
  private static final String INSTANCE_OPEN_OUTPUT_LANE_KEY = "validation.instance.open.output.lane";
  private static final String INSTANCE_OPEN_OUTPUT_LANE_DEFAULT = "Instance has an open lane '%s'";
  private static final String STAGE_CANNOT_HAVE_INPUT_KEY = "validation.stage.cannot.have.input.lanes";
  private static final String STAGE_CANNOT_HAVE_INPUT_DEFAULT = "%s cannot have input lanes '%s'";
  private static final String STAGE_CANNOT_HAVE_OUTPUT_KEY = "validation.stage.cannot.have.output.lanes";
  private static final String STAGE_CANNOT_HAVE_OUTPUT_DEFAULT = "%s cannot have output lanes '%s'";
  private static final String STAGE_MUST_HAVE_INPUT_KEY = "validation.stage.must.have.input.lanes";
  private static final String STAGE_MUST_HAVE_INPUT_DEFAULT = "%s must have input lanes";
  private static final String STAGE_MUST_HAVE_OUTPUT_KEY = "validation.stage.must.have.output.lanes";
  private static final String STAGE_MUST_HAVE_OUTPUT_DEFAULT = "%s must have output lanes";
  private static final String STAGE_INVALID_INSTANCE_NAME_KEY = "validation.invalid.instance.name";
  private static final String STAGE_INVALID_INSTANCE_NAME_DEFAULT =
      "Invalid instance name, names can only contain the following characters '%s'";
  private static final String STAGE_INVALID_INPUT_LANE_NAME_KEY = "validation.invalid.input.lane.name";
  private static final String STAGE_INVALID_INPUT_LANE_DEFAULT =
      "Invalid input lane names '%s', lanes can only contain the following characters '%s'";
  private static final String STAGE_INVALID_OUTPUT_LANE_NAME_KEY = "validation.invalid.output.lane.name";
  private static final String STAGE_INVALID_OUTPUT_LANE_DEFAULT =
      "Invalid output lane names '%s', lanes can only contain the following characters '%s'";

  private static final String VALID_NAME= "[0-9A-Za-z_]";

  private final StageLibrary stageLibrary;
  private final PipelineConfiguration pipelineConfiguration;
  private final Issues issues;
  private final List<String> openLanes;
  private boolean validated;
  private boolean canPreview;

  public PipelineConfigurationValidator(StageLibrary stageLibrary, PipelineConfiguration pipelineConfiguration) {
    Preconditions.checkNotNull(stageLibrary, "stageLibrary cannot be null");
    Preconditions.checkNotNull(pipelineConfiguration, "pipelineConfiguration cannot be null");
    this.stageLibrary = stageLibrary;
    this.pipelineConfiguration = pipelineConfiguration;
    issues = new Issues();
    openLanes = new ArrayList<String>();
  }

  public boolean validate() {
    Preconditions.checkState(!validated, "Already validated");
    validated = true;
    canPreview = checkIfPipelineIsEmpty();
    canPreview &= validatePipelineConfiguration();
    canPreview &= validatePipelineLanes();
    return !issues.hasIssues();
  }

  public boolean canPreview() {
    Preconditions.checkState(validated, String.format("validate() has not been called"));
    return canPreview;
  }

  public Issues getIssues() {
    Preconditions.checkState(validated, String.format("validate() has not been called"));
    return issues;
  }

  public List<String> getOpenLanes() {
    Preconditions.checkState(validated, String.format("validate() has not been called"));
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
        issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                               INSTANCE_ALREADY_DEFINED_KEY, INSTANCE_ALREADY_DEFINED_DEFAULT));
        preview = false;
      }
      StageDefinition stageDef = stageLibrary.getStage(stageConf.getLibrary(), stageConf.getStageName(),
                                                       stageConf.getStageVersion());
      if (stageDef == null) {
        issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                  STAGE_DOES_NOT_EXIST_KEY, STAGE_DOES_NOT_EXIST_DEFAULT,
                                  stageConf.getLibrary(), stageConf.getStageName(), stageConf.getStageVersion()));
        preview = false;
      } else {
        if (shouldBeSource) {
          if (stageDef.getType() != StageType.SOURCE) {
            issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                      FIRST_STAGE_MUST_BE_A_SOURCE_KEY, FIRST_STAGE_MUST_BE_A_SOURCE_DEFAULT));
            preview = false;
          }
        } else {
          if (stageDef.getType() == StageType.SOURCE) {
            issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                      STAGE_CANNOT_BE_SOURCE_KEY, STAGE_CANNOT_BE_SOURCE_DEFAULT));
            preview = false;
          }
        }
        shouldBeSource = false;
        if (!stageConf.isSystemGenerated() && !isValidName(stageConf.getInstanceName())) {
          issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                    STAGE_INVALID_INSTANCE_NAME_KEY, STAGE_INVALID_INSTANCE_NAME_DEFAULT, VALID_NAME));
          preview = false;
        }
        for (String lane : stageConf.getInputLanes()) {
          if (!isValidName(lane)) {
            issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                      STAGE_INVALID_INPUT_LANE_NAME_KEY, STAGE_INVALID_INPUT_LANE_DEFAULT, lane,
                                      VALID_NAME));
            preview = false;
          }
        }
        for (String lane : stageConf.getOutputLanes()) {
          if (!isValidName(lane)) {
            issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                      STAGE_INVALID_OUTPUT_LANE_NAME_KEY, STAGE_INVALID_OUTPUT_LANE_DEFAULT, lane,
                                      VALID_NAME));
            preview = false;
          }
        }
        switch (stageDef.getType()) {
          case SOURCE:
            if (!stageConf.getInputLanes().isEmpty()) {
              issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                        STAGE_CANNOT_HAVE_INPUT_KEY, STAGE_CANNOT_HAVE_INPUT_DEFAULT,
                                        stageDef.getType(), stageConf.getInputLanes()));
              preview = false;
            }
            if (stageConf.getOutputLanes().isEmpty()) {
              issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                                     STAGE_MUST_HAVE_OUTPUT_KEY, STAGE_MUST_HAVE_OUTPUT_DEFAULT,
                                                     stageDef.getType()));
            }
            break;
          case PROCESSOR:
            if (stageConf.getInputLanes().isEmpty()) {
              issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                        STAGE_MUST_HAVE_INPUT_KEY, STAGE_MUST_HAVE_INPUT_DEFAULT, stageDef.getType()));
              preview = false;
            }
            if (stageConf.getOutputLanes().isEmpty()) {
              issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                        STAGE_MUST_HAVE_OUTPUT_KEY, STAGE_MUST_HAVE_OUTPUT_DEFAULT,
                                        stageDef.getType()));
            }
            break;
          case TARGET:
            if (stageConf.getInputLanes().isEmpty()) {
              issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                                     STAGE_MUST_HAVE_INPUT_KEY, STAGE_MUST_HAVE_INPUT_DEFAULT,
                                                     stageDef.getType()));
              preview = false;
            }
            if (!stageConf.getOutputLanes().isEmpty()) {
              issues.add(StageIssue.createStageIssue(stageConf.getInstanceName(),
                                        STAGE_CANNOT_HAVE_OUTPUT_KEY, STAGE_CANNOT_HAVE_OUTPUT_DEFAULT,
                                        stageDef.getType(), stageConf.getOutputLanes()));
              preview = false;
            }
            break;
        }
        for (ConfigDefinition confDef : stageDef.getConfigDefinitions()) {
          if (stageConf.getConfig(confDef.getName()) == null && confDef.isRequired()) {
            issues.add(StageIssue.createConfigIssue(stageConf.getInstanceName(), confDef.getName(),
                                                    STAGE_MISSING_CONFIGURATION_KEY,
                                                    STAGE_MISSING_CONFIGURATION_DEFAULT));
            preview = false;
          }
        }
        for (ConfigConfiguration conf : stageConf.getConfiguration()) {
          ConfigDefinition confDef = stageDef.getConfigDefinition(conf.getName());
          if (conf.getValue() == null && confDef.isRequired()) {
            issues.add(StageIssue.createConfigIssue(stageConf.getInstanceName(), confDef.getName(),
                                      STAGE_MISSING_CONFIGURATION_KEY, STAGE_MISSING_CONFIGURATION_DEFAULT));
            preview = false;
          }
          if (conf.getValue() != null) {
            switch (confDef.getType()) {
              case BOOLEAN:
                if (!(conf.getValue() instanceof Boolean)) {
                  issues.add(StageIssue.createConfigIssue(stageConf.getInstanceName(), confDef.getName(),
                                            STAGE_CONFIGURATION_INVALID_TYPE_KEY,
                                            STAGE_CONFIGURATION_INVALID_TYPE_DEFAULT, confDef.getType()));
                  preview = false;
                }
                break;
              case INTEGER:
                if (!(conf.getValue() instanceof Long || conf.getValue() instanceof Integer)) {
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
                if (!(conf.getValue() instanceof Map || conf.getValue() instanceof List)) {
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
    Set<String> output = new HashSet<String>();
    Set<String> input = new HashSet<String>();
    for (StageConfiguration stage : pipelineConfiguration.getStages()) {
      output.addAll(stage.getOutputLanes());
      input.addAll(stage.getInputLanes());
    }
    Set<String> open = new HashSet<String>(output);
    open.removeAll(input);
    openLanes.addAll(open);
    if (!open.isEmpty()) {
      for (String lane : open) {
        for (StageConfiguration stage : pipelineConfiguration.getStages()) {
          if (stage.getOutputLanes().contains(lane)) {
            issues.add(StageIssue.createStageIssue(stage.getInstanceName(), INSTANCE_OPEN_OUTPUT_LANE_KEY,
                                                   INSTANCE_OPEN_OUTPUT_LANE_DEFAULT, lane));
          }
        }
      }
    }
    return preview;
  }

}
