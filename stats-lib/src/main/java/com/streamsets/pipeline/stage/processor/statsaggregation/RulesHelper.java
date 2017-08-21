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

import com.streamsets.datacollector.config.RuleDefinition;
import com.streamsets.datacollector.util.AggregatorUtil;
import com.streamsets.pipeline.api.Record;

import java.util.Map;

public class RulesHelper {

  static boolean isRuleDefinitionLatest(Map<String, RuleDefinition> ruleDefinitionMap, Record record) {
    // Determines if the Rule Definition Change/Disabled Record is latest
    boolean isLatest = false;
    long timestampOnRecord = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.TIMESTAMP).getValueAsLong();
    RuleDefinition ruleDefinition = ruleDefinitionMap.get(
      record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.RULE_ID).getValueAsString()
    );
    if (ruleDefinition == null) {
      isLatest = true;
    } else {
      Long timestampOnRule = ruleDefinition.getTimestamp();
      if (timestampOnRecord > timestampOnRule) {
        isLatest = true;
      }
    }
    return isLatest;
  }

  static boolean isDataRuleRecordValid(Map<String, RuleDefinition> ruleDefinitionMap, Record record) {
    // data rule record is considered valid if the timestamp of the record is greater than the
    // timestamp on the corresponding rule definition
    String ruleId = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.RULE_ID).getValueAsString();
    long recordTimeStamp = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.TIMESTAMP).getValueAsLong();
    RuleDefinition ruleDefinition = ruleDefinitionMap.get(ruleId);

    boolean valid = true;
    if (ruleDefinition == null) {
      valid = false;
    } else if (ruleDefinition.getTimestamp() >= recordTimeStamp) {
      // rule definition is newer than the record, so record it not valid
      valid = false;
    }
    return valid;
  }


}
