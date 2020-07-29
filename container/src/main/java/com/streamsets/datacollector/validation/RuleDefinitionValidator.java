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
package com.streamsets.datacollector.validation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.ThresholdType;
import com.streamsets.datacollector.configupgrade.RuleDefinitionsUpgrader;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.definition.ConcreteELDefinitionExtractor;
import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.datacollector.el.RuleELRegistry;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FieldVisitor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.lib.el.RecordEL;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RuleDefinitionValidator {

  private static final Logger LOG = LoggerFactory.getLogger(RuleDefinitionValidator.class);

  private static final String LABEL = "label";
  private static final String CONDITION = "condition";
  private static final String VAL = "value()";
  private static final String TIME_NOW = "time:now()";
  private static final String START_TIME = "pipeline:startTime()";
  private static final String METRIC_ID = "metric id";
  private static final String DEFAULT_VALUE = "10";
  private static final String PROPERTY = "property";
  private static final int MIN_PERCENTAGE = 0;
  private static final int MAX_PERCENTAGE = 100;
  private static final String THRESHOLD_VALUE = "Threshold Value";
  private static final String EMAIL_IDS = "emailIds";
  private static final String SAMPLING_PERCENTAGE = "Sampling Percentage";

  private final String pipelineId;
  private final RuleDefinitions ruleDefinitions;
  private final Map<String, Object> pipelineParameters;
  private final ELEvaluator elEvaluator;
  private final ELVariables variables;

  public RuleDefinitionValidator(
      String pipelineId,
      RuleDefinitions ruleDefinitions,
      Map<String, Object> pipelineParameters
  ) {
    this.pipelineId = Preconditions.checkNotNull(pipelineId, "pipelineId cannot be null");
    this.ruleDefinitions = Preconditions.checkNotNull(ruleDefinitions, "ruleDefinitions cannot be null");
    this.pipelineParameters = pipelineParameters;
    variables = new ELVariables();
    elEvaluator = new ELEvaluator("RuleDefinitionValidator", false, ConcreteELDefinitionExtractor.get(), RuleELRegistry.getRuleELs(RuleELRegistry.GENERAL));
  }

  public boolean validateRuleDefinition() {
    List<RuleIssue> ruleIssues = new ArrayList<>();

    List<Issue> configIssues = new ArrayList<>();
    configIssues.addAll(upgradeRuleDefinitions());
    RuleDefinitionsConfigBean ruleDefinitionsConfigBean = PipelineBeanCreator.get()
        .createRuleDefinitionsConfigBean(ruleDefinitions, configIssues, pipelineParameters);
    List<String> emailIds = ruleDefinitionsConfigBean.emailIDs;

    for(DataRuleDefinition dataRuleDefinition : ruleDefinitions.getDataRuleDefinitions()) {
      //reset valid flag before validating
      dataRuleDefinition.setValid(true);
      String ruleId = dataRuleDefinition.getId();
      if(dataRuleDefinition.getLabel() == null || dataRuleDefinition.getLabel().isEmpty()) {
        RuleIssue r = RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0040, LABEL);
        r.setAdditionalInfo(PROPERTY, LABEL);
        ruleIssues.add(r);
        dataRuleDefinition.setValid(false);
      }
      if(dataRuleDefinition.getCondition() == null || dataRuleDefinition.getCondition().isEmpty()) {
        RuleIssue r = RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0040, CONDITION);
        r.setAdditionalInfo(PROPERTY, CONDITION);
        ruleIssues.add(r);
        dataRuleDefinition.setValid(false);
      } else {
        //validate the condition el expression
        RuleIssue issue = validateDataRuleExpressions(dataRuleDefinition.getCondition(), ruleId);
        if(issue != null) {
          issue.setAdditionalInfo(PROPERTY, CONDITION);
          ruleIssues.add(issue);
          dataRuleDefinition.setValid(false);
        }
      }
      if(dataRuleDefinition.getSamplingPercentage() < MIN_PERCENTAGE ||
        dataRuleDefinition.getSamplingPercentage() > MAX_PERCENTAGE) {
        RuleIssue r = RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0041);
        r.setAdditionalInfo(PROPERTY, SAMPLING_PERCENTAGE);
        ruleIssues.add(r);
        dataRuleDefinition.setValid(false);
      }
      if(dataRuleDefinition.isSendEmail() && CollectionUtils.isEmpty(emailIds)) {
        RuleIssue r = RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0042);
        r.setAdditionalInfo(PROPERTY, EMAIL_IDS);
        ruleIssues.add(r);
        dataRuleDefinition.setValid(false);
      }
      double threshold;
      try {
        threshold = Double.parseDouble(dataRuleDefinition.getThresholdValue());
        if(dataRuleDefinition.getThresholdType() == ThresholdType.PERCENTAGE) {
          if(threshold < MIN_PERCENTAGE || threshold > MAX_PERCENTAGE) {
            RuleIssue r = RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0044);
            r.setAdditionalInfo(PROPERTY, THRESHOLD_VALUE);
            ruleIssues.add(r);
            dataRuleDefinition.setValid(false);
          }
        }
      } catch (NumberFormatException e) {
        RuleIssue r = RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0043);
        r.setAdditionalInfo(PROPERTY, THRESHOLD_VALUE);
        ruleIssues.add(r);
        dataRuleDefinition.setValid(false);
      }
    }

    for(MetricsRuleDefinition metricsRuleDefinition : ruleDefinitions.getMetricsRuleDefinitions()) {
      String ruleId = metricsRuleDefinition.getId();
      //reset valid flag before validating
      metricsRuleDefinition.setValid(true);
      if(metricsRuleDefinition.getAlertText() == null || metricsRuleDefinition.getAlertText().isEmpty()) {
        RuleIssue r = RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0050, LABEL);
        r.setAdditionalInfo(PROPERTY, LABEL);
        ruleIssues.add(r);

        metricsRuleDefinition.setValid(false);
      }
      if(metricsRuleDefinition.getMetricId() == null || metricsRuleDefinition.getMetricId().isEmpty()) {
        RuleIssue r = RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0050, METRIC_ID);
        r.setAdditionalInfo(PROPERTY, METRIC_ID);
        ruleIssues.add(r);

        metricsRuleDefinition.setValid(false);
      }
      if(metricsRuleDefinition.getCondition() == null || metricsRuleDefinition.getCondition().isEmpty()) {
        RuleIssue r = RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0050, CONDITION);
        r.setAdditionalInfo(PROPERTY, CONDITION);
        ruleIssues.add(r);
        metricsRuleDefinition.setValid(false);
      } else {
        //validate the condition el expression
        RuleIssue issue = validateMetricAlertExpressions(metricsRuleDefinition.getCondition(), ruleId);
        if(issue != null) {
          issue.setAdditionalInfo(PROPERTY, CONDITION);
          ruleIssues.add(issue);
          metricsRuleDefinition.setValid(false);
        }
      }
      if(metricsRuleDefinition.isSendEmail() && CollectionUtils.isEmpty(emailIds)) {
        RuleIssue r = RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0042);
        r.setAdditionalInfo(PROPERTY, EMAIL_IDS);
        ruleIssues.add(r);
        metricsRuleDefinition.setValid(false);
      }
    }

    ruleDefinitions.setRuleIssues(ruleIssues);
    ruleDefinitions.setConfigIssues(configIssues);
    return ruleIssues.size() == 0 && configIssues.size() == 0;
  }

  private RuleIssue validateMetricAlertExpressions(String condition, String ruleId){
    if(!condition.startsWith("${") || !condition.endsWith("}") || !condition.contains(VAL)) {
      return RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0046);
    }
    String predicateWithValue = condition
      .replace(VAL, DEFAULT_VALUE)
      .replace(TIME_NOW, DEFAULT_VALUE)
      .replace(START_TIME, DEFAULT_VALUE);
    try {
      elEvaluator.eval(variables, predicateWithValue, Object.class);
    } catch (ELEvalException e) {
      return RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0047, condition);
    }
    return null;
  }

  private RuleIssue validateDataRuleExpressions(String condition, String ruleId){
    Record record = new Record(){
      @Override
      public Header getHeader() {
        return new Header() {
          @Override
          public String getStageCreator() {
            return null;
          }

          @Override
          public String getSourceId() {
            return null;
          }

          @Override
          public String getTrackingId() {
            return null;
          }

          @Override
          public String getPreviousTrackingId() {
            return null;
          }

          @Override
          public String getStagesPath() {
            return null;
          }

          @Override
          public byte[] getRaw() {
            return new byte[0];
          }

          @Override
          public String getRawMimeType() {
            return null;
          }

          @Override
          public Set<String> getAttributeNames() {
            return null;
          }

          @Override
          public String getAttribute(String name) {
            return null;
          }

          @Override
          public void setAttribute(String name, String value) {

          }

          @Override
          public void deleteAttribute(String name) {

          }

          @Override
          public String getErrorDataCollectorId() {
            return null;
          }

          @Override
          public String getErrorPipelineName() {
            return null;
          }

          @Override
          public String getErrorCode() {
            return null;
          }

          @Override
          public String getErrorMessage() {
            return null;
          }

          @Override
          public String getErrorStage() {
            return null;
          }

          @Override
          public String getErrorStageLabel() {
            return null;
          }

          @Override
          public long getErrorTimestamp() {
            return 0;
          }

          @Override
          public String getErrorStackTrace() {
            return null;
          }

          @Override
          public String getErrorJobId() {
            return null;
          }

          @Override
          public String getErrorJobName() {
            return null;
          }

          /** To be removed */
          public Map<String, Object> getAllAttributes() {
            return null;
          }

          /** To be removed */
          public Map<String, Object> overrideUserAndSystemAttributes(Map<String, Object> newAttrs) {
            return null;
          }

          /** To be removed */
          public Map<String, Object> getUserAttributes() {return null;}

          /** To be removed */
          public Map<String, Object> setUserAttributes(Map<String, Object> newAttributes) {return null;}

        };
      }

      @Override
      public Field get() {
        return null;
      }

      @Override
      public Field set(Field field) {
        return null;
      }

      @Override
      public Field get(String fieldPath) {
        return null;
      }

      @Override
      public Field delete(String fieldPath) {
        return null;
      }

      @Override
      public boolean has(String fieldPath) {
        return false;
      }

      @Override
      @Deprecated
      public Set<String> getFieldPaths() {
        return null;
      }

      @Override
      public Set<String> getEscapedFieldPaths() {
        return null;
      }

      @Override
      public List<String> getEscapedFieldPathsOrdered() {
        return null;
      }

      @Override
      public Field set(String fieldPath, Field newField) {
        return null;
      }

      @Override
      public void forEachField(FieldVisitor visitor) throws StageException {
      }

    };

    RecordEL.setRecordInContext(variables, record);
    try {
      elEvaluator.eval(variables, condition, Object.class);
    } catch (ELEvalException ex) {
      return RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0045, condition, ex.toString());
    }
    return null;
  }

  @VisibleForTesting
  RuleDefinitionsUpgrader getUpgrader() {
    return RuleDefinitionsUpgrader.get();
  }

  private List<Issue> upgradeRuleDefinitions() {
    List<Issue> upgradeIssues = new ArrayList<>();
    RuleDefinitions upgradedRuleDefinitions = getUpgrader().upgradeIfNecessary(
        pipelineId,
        ruleDefinitions,
        upgradeIssues
    );
    return upgradeIssues;
  }

}
