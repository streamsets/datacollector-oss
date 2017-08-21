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

import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.RuleDefinition;
import com.streamsets.datacollector.util.AggregatorUtil;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class TestRulesHelper {

  @Test
  public void testIsRuleDefinitionLatest() {
    long t2 = System.currentTimeMillis();
    long t1 = t2-1;
    long t3 = t2+1;

    DataRuleDefinition dataRuleDefinition = TestHelper.createTestDataRuleDefinition("a < b", true, t2);
    Map<String, RuleDefinition> ruleDefinitionMap = new HashMap<>();
    ruleDefinitionMap.put(dataRuleDefinition.getId(), dataRuleDefinition);

    Record oldRecord = Mockito.mock(Record.class);
    Mockito.when(oldRecord.get("/" + AggregatorUtil.TIMESTAMP)).thenReturn(Field.create(t1));
    Mockito.when(oldRecord.get("/" + AggregatorUtil.RULE_ID)).thenReturn(Field.create("x"));
    Assert.assertFalse(RulesHelper.isDataRuleRecordValid(ruleDefinitionMap, oldRecord));

    Record newRecord = Mockito.mock(Record.class);
    Mockito.when(newRecord.get("/" + AggregatorUtil.TIMESTAMP)).thenReturn(Field.create(t3));
    Mockito.when(newRecord.get("/" + AggregatorUtil.RULE_ID)).thenReturn(Field.create("x"));
    Assert.assertTrue(RulesHelper.isDataRuleRecordValid(ruleDefinitionMap, newRecord));

  }

  @Test
  public void testIsDataRuleRecordValid() {
    long t2 = System.currentTimeMillis();
    long t1 = t2-1;
    long t3 = t2+1;

    DataRuleDefinition dataRuleDefinition = TestHelper.createTestDataRuleDefinition("a < b", true, t2);
    Map<String, RuleDefinition> ruleDefinitionMap = new HashMap<>();
    ruleDefinitionMap.put(dataRuleDefinition.getId(), dataRuleDefinition);

    Record oldRecord = Mockito.mock(Record.class);
    Mockito.when(oldRecord.get("/" + AggregatorUtil.TIMESTAMP)).thenReturn(Field.create(t1));
    Mockito.when(oldRecord.get("/" + AggregatorUtil.RULE_ID)).thenReturn(Field.create("x"));
    Assert.assertFalse(RulesHelper.isDataRuleRecordValid(ruleDefinitionMap, oldRecord));

    Record newRecord = Mockito.mock(Record.class);
    Mockito.when(newRecord.get("/" + AggregatorUtil.TIMESTAMP)).thenReturn(Field.create(t3));
    Mockito.when(newRecord.get("/" + AggregatorUtil.RULE_ID)).thenReturn(Field.create("x"));
    Assert.assertTrue(RulesHelper.isDataRuleRecordValid(ruleDefinitionMap, newRecord));

  }
}
