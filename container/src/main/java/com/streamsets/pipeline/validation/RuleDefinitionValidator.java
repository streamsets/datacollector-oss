/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.validation;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataRuleDefinition;
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
    for(DataRuleDefinition dataRuleDefinition : ruleDefinition.getDataRuleDefinitions()) {
      String ruleId = dataRuleDefinition.getId();
      if(dataRuleDefinition.getLabel() == null || dataRuleDefinition.getLabel().isEmpty()) {
        ruleIssues.add(RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0040, LABEL));
      }
      if(dataRuleDefinition.getCondition() == null || dataRuleDefinition.getCondition().isEmpty()) {
        ruleIssues.add(RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0040, CONDITION));
      } else {
        //validate the condition el expression
        RuleIssue issue = validateExpressions(dataRuleDefinition.getCondition(), ruleId);
        if(issue != null) {
          ruleIssues.add(issue);
        }
      }
      if(dataRuleDefinition.getSamplingPercentage() < 0 || dataRuleDefinition.getSamplingPercentage() > 100) {
        ruleIssues.add(RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0041));
      }
      if(dataRuleDefinition.isSendEmail() &&
        (dataRuleDefinition.getEmailIds() == null || dataRuleDefinition.getEmailIds().isEmpty())) {
        ruleIssues.add(RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0042));
      }
      double threshold;
      try {
        threshold = Double.parseDouble(dataRuleDefinition.getThresholdValue());
        if(dataRuleDefinition.getThresholdType() == ThresholdType.PERCENTAGE) {
          if(threshold < 0 || threshold > 100) {
            ruleIssues.add(RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0044));
          }
        }
      } catch (NumberFormatException e) {
        ruleIssues.add(RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0043));
      }
    }

    for(MetricsAlertDefinition metricsAlertDefinition : ruleDefinition.getMetricsAlertDefinitions()) {
      String ruleId = metricsAlertDefinition.getId();
      if(metricsAlertDefinition.getLabel() == null || metricsAlertDefinition.getLabel().isEmpty()) {
        ruleIssues.add(RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0050, LABEL));
      }
      if(metricsAlertDefinition.getMetricId() == null || metricsAlertDefinition.getMetricId().isEmpty()) {
        ruleIssues.add(RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0050, "metric id"));
      }
      if(metricsAlertDefinition.getCondition() == null || metricsAlertDefinition.getCondition().isEmpty()) {
        ruleIssues.add(RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0050, "condition"));
      } else {
        //validate the condition el expression
        RuleIssue issue = validateExpressions(metricsAlertDefinition.getCondition(), ruleId);
        if(issue != null) {
          ruleIssues.add(issue);
        }
      }
    }

    ruleDefinition.setRuleIssues(ruleIssues);
    return ruleIssues.size() == 0? true : false;
  }

  private RuleIssue validateExpressions(String condition, String ruleId){
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
      RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0045, condition);
    }
    return null;
  }

}
