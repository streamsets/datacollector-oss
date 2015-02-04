/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.validation;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.config.DataRuleDefinition;
import com.streamsets.pipeline.config.MetricsAlertDefinition;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.config.ThresholdType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RuleDefinitionValidator {

  private static final Logger LOG = LoggerFactory.getLogger(RuleDefinitionValidator.class);

  public boolean validateRuleDefinition(RuleDefinition ruleDefinition) {
    Preconditions.checkNotNull(ruleDefinition);

    List<RuleIssue> ruleIssues = new ArrayList<>();
    for(DataRuleDefinition dataRuleDefinition : ruleDefinition.getDataRuleDefinitions()) {
      String ruleId = dataRuleDefinition.getId();
      if(dataRuleDefinition.getLabel() == null || dataRuleDefinition.getLabel().isEmpty()) {
        ruleIssues.add(RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0040, "label"));
      }
      if(dataRuleDefinition.getCondition() == null || dataRuleDefinition.getCondition().isEmpty()) {
        ruleIssues.add(RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0040, "condition"));
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
        ruleIssues.add(RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0050, "label"));
      }
      if(metricsAlertDefinition.getMetricId() == null || metricsAlertDefinition.getMetricId().isEmpty()) {
        ruleIssues.add(RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0050, "metric id"));
      }
      if(metricsAlertDefinition.getCondition() == null || metricsAlertDefinition.getCondition().isEmpty()) {
        ruleIssues.add(RuleIssue.createRuleIssue(ruleId, ValidationError.VALIDATION_0050, "condition"));
      }
    }

    ruleDefinition.setRuleIssues(ruleIssues);
    return ruleIssues.size() == 0? true : false;
  }

}
