/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.validation;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataAlertDefinition;
import com.streamsets.pipeline.config.MetricsAlertDefinition;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.config.ThresholdType;
import com.streamsets.pipeline.el.ELBasicSupport;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.el.ELStringSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class RuleDefinitionValidator {

  private static final Logger LOG = LoggerFactory.getLogger(RuleDefinitionValidator.class);

  private static final String LABEL = "label";
  private static final String CONDITION = "condition";
  private static final String VAL = "value()";
  private static final String METRIC_ID = "metric id";
  private static final String DEFAULT_VALUE = "10";
  private static final String PROPERTY = "property";
  private static final int MIN_PERCENTAGE = 0;
  private static final int MAX_PERCENTAGE = 100;
  private static final String THRESHOLD_VALUE = "Threshold Value";
  private static final String EMAIL_IDS = "Email Ids";
  private static final String SAMPLING_PERCENTAGE = "Sampling Percentage";

  private final ELEvaluator elEvaluator;
  private final ELEvaluator.Variables variables;

  public RuleDefinitionValidator() {
    variables = new ELEvaluator.Variables();
    elEvaluator = new ELEvaluator();
    ELBasicSupport.registerBasicFunctions(elEvaluator);
    ELRecordSupport.registerRecordFunctions(elEvaluator);
    ELStringSupport.registerStringFunctions(elEvaluator);
  }

  public boolean validateRuleDefinition(RuleDefinition ruleDefinition) {
    Preconditions.checkNotNull(ruleDefinition);

    List<RuleIssue> ruleIssues = new ArrayList<>();
    for(DataAlertDefinition dataAlertDefinition : ruleDefinition.getDataAlertDefinitions()) {
      //reset valid flag before validating
      dataAlertDefinition.setValid(true);
      String ruleId = dataAlertDefinition.getId();
      if(dataAlertDefinition.getLabel() == null || dataAlertDefinition.getLabel().isEmpty()) {
        RuleIssue r = RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0040, LABEL);
        r.setAdditionalInfo(PROPERTY, LABEL);
        ruleIssues.add(r);
        dataAlertDefinition.setValid(false);
      }
      if(dataAlertDefinition.getCondition() == null || dataAlertDefinition.getCondition().isEmpty()) {
        RuleIssue r = RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0040, CONDITION);
        r.setAdditionalInfo(PROPERTY, CONDITION);
        ruleIssues.add(r);
        dataAlertDefinition.setValid(false);
      } else {
        //validate the condition el expression
        RuleIssue issue = validateDataRuleExpressions(dataAlertDefinition.getCondition(), ruleId);
        if(issue != null) {
          issue.setAdditionalInfo(PROPERTY, CONDITION);
          ruleIssues.add(issue);
          dataAlertDefinition.setValid(false);
        }
      }
      if(dataAlertDefinition.getSamplingPercentage() < MIN_PERCENTAGE ||
        dataAlertDefinition.getSamplingPercentage() > MAX_PERCENTAGE) {
        RuleIssue r = RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0041);
        r.setAdditionalInfo(PROPERTY, SAMPLING_PERCENTAGE);
        ruleIssues.add(r);
        dataAlertDefinition.setValid(false);
      }
      if(dataAlertDefinition.isSendEmail() &&
        (ruleDefinition.getEmailIds() == null || ruleDefinition.getEmailIds().isEmpty())) {
        RuleIssue r = RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0042);
        r.setAdditionalInfo(PROPERTY, EMAIL_IDS);
        ruleIssues.add(r);
        dataAlertDefinition.setValid(false);
      }
      double threshold;
      try {
        threshold = Double.parseDouble(dataAlertDefinition.getThresholdValue());
        if(dataAlertDefinition.getThresholdType() == ThresholdType.PERCENTAGE) {
          if(threshold < MIN_PERCENTAGE || threshold > MAX_PERCENTAGE) {
            RuleIssue r = RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0044);
            r.setAdditionalInfo(PROPERTY, THRESHOLD_VALUE);
            ruleIssues.add(r);
            dataAlertDefinition.setValid(false);
          }
        }
      } catch (NumberFormatException e) {
        RuleIssue r = RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0043);
        r.setAdditionalInfo(PROPERTY, THRESHOLD_VALUE);
        ruleIssues.add(r);
        dataAlertDefinition.setValid(false);
      }
    }

    for(MetricsAlertDefinition metricsAlertDefinition : ruleDefinition.getMetricsAlertDefinitions()) {
      String ruleId = metricsAlertDefinition.getId();
      //reset valid flag before validating
      metricsAlertDefinition.setValid(true);
      if(metricsAlertDefinition.getAlertText() == null || metricsAlertDefinition.getAlertText().isEmpty()) {
        RuleIssue r = RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0050, LABEL);
        r.setAdditionalInfo(PROPERTY, LABEL);
        ruleIssues.add(r);

        metricsAlertDefinition.setValid(false);
      }
      if(metricsAlertDefinition.getMetricId() == null || metricsAlertDefinition.getMetricId().isEmpty()) {
        RuleIssue r = RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0050, METRIC_ID);
        r.setAdditionalInfo(PROPERTY, METRIC_ID);
        ruleIssues.add(r);

        metricsAlertDefinition.setValid(false);
      }
      if(metricsAlertDefinition.getCondition() == null || metricsAlertDefinition.getCondition().isEmpty()) {
        RuleIssue r = RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0050, CONDITION);
        r.setAdditionalInfo(PROPERTY, CONDITION);
        ruleIssues.add(r);
        metricsAlertDefinition.setValid(false);
      } else {
        //validate the condition el expression
        RuleIssue issue = validateMetricAlertExpressions(metricsAlertDefinition.getCondition(), ruleId);
        if(issue != null) {
          issue.setAdditionalInfo(PROPERTY, CONDITION);
          ruleIssues.add(issue);
          metricsAlertDefinition.setValid(false);
        }
      }
    }

    ruleDefinition.setRuleIssues(ruleIssues);
    return ruleIssues.size() == 0 ? true : false;
  }

  private RuleIssue validateMetricAlertExpressions(String condition, String ruleId){
    if(!condition.startsWith("${") || !condition.endsWith("}") || !condition.contains(VAL)) {
      return RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0046);
    }
    String predicateWithValue = condition.replace(VAL, DEFAULT_VALUE);
    try {
      elEvaluator.eval(variables, predicateWithValue);
    } catch (ELException ex) {
      return RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0045, condition);
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
          public long getErrorTimestamp() {
            return 0;
          }
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
      public Set<String> getFieldPaths() {
        return null;
      }

      @Override
      public Field set(String fieldPath, Field newField) {
        return null;
      }
    };

    ELRecordSupport.setRecordInContext(variables, record);
    try {
      elEvaluator.eval(variables, condition);
    } catch (ELException ex) {
      return RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0045, condition);
    }
    return null;
  }

}
