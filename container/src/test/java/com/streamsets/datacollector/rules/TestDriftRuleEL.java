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
package com.streamsets.datacollector.rules;

import com.streamsets.datacollector.definition.ConcreteELDefinitionExtractor;
import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.datacollector.execution.alerts.DataRuleEvaluator;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import org.junit.Assert;
import org.junit.Test;
import org.junit.matchers.JUnitMatchers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("checked")
public class TestDriftRuleEL {

  @Test
  public void testDriftDetector() throws Exception {
    DriftRuleEL.DriftDetector<String> detector = new DriftRuleEL.DriftDetector<String>() {
      private final Record record = new RecordImpl("c", "id", null, null);
      private final Map<String, Object> context = new HashMap<>();

      @Override
      Record getRecord() {
        return record;
      }

      @Override
      Map<String, Object> getContext() {
        return context;
      }

      @Override
      public Set<Field.Type> supportedTypes() {
        return null;
      }

      @Override
      protected String getContextPrefix() {
        return "test";
      }

      @Override
      protected String getValue(Field field) {
        return field.getValueAsString();
      }

      @Override
      protected String composeAlert(String fieldPath, String stored, String inRecord) {
        return fieldPath + ":" + stored + ":" + inRecord;
      }

    };

    // ignore missing asserts

    detector.getContext().clear();

    // NULL value, first record
    Assert.assertFalse(detector.detect("/", true));

    // NULL value, follow up record
    Assert.assertFalse(detector.detect("/", true));

    // NOT NULL value, first record
    detector.getContext().clear();
    detector.getRecord().set(Field.create("foo"));
    Assert.assertFalse(detector.detect("/", true));

    // NOT NULL value, follow up record, no drift
    Assert.assertFalse(detector.detect("/", true));

    // NULL value, follow up record
    detector.getRecord().set(null);
    Assert.assertFalse(detector.detect("/", true));

    // NOT NULL, follow up record, no drift
    detector.getRecord().set(Field.create("foo"));
    Assert.assertFalse(detector.detect("/", true));

    // NOT NULL, follow up record,  drift
    detector.getRecord().set(Field.create("bar"));
    Assert.assertTrue(detector.detect("/", true));

    // NOT NULL, follow up record,  stable after drift
    Assert.assertFalse(detector.detect("/", true));

    // NULL value, follow up record
    detector.getRecord().set(null);
    Assert.assertFalse(detector.detect("/", true));


    // don't ignore missing asserts

    detector.getContext().clear();

    // NULL value, first record
    Assert.assertFalse(detector.detect("/", false));

    // NULL value, follow up record
    Assert.assertFalse(detector.detect("/", false));

    // NOT NULL value, follow up record
    detector.getRecord().set(Field.create("foo"));
    Assert.assertTrue(detector.detect("/", false));

    // NULL value, follow up record
    detector.getRecord().set(null);
    Assert.assertTrue(detector.detect("/", false));

    // NOT NULL value, first record
    detector.getContext().clear();
    detector.getRecord().set(Field.create("foo"));
    Assert.assertFalse(detector.detect("/", false));

    // NOT NULL value, follow up record, no drift
    Assert.assertFalse(detector.detect("/", false));

    // NOT NULL, follow up record,  drift
    detector.getRecord().set(Field.create("bar"));
    Assert.assertTrue(detector.detect("/", false));

    // NOT NULL, follow up record,  stable after drift
    Assert.assertFalse(detector.detect("/", false));

    // NULL value, follow up record
    detector.getRecord().set(null);
    Assert.assertTrue(detector.detect("/", false));
  }

    @Test
  public void testDriftSize() throws Exception {
    ELVars vars = new ELVariables();

    Record record = new RecordImpl("creator", "id", null, null);
    record.set(Field.create(new ArrayList<Field>()));

    RecordEL.setRecordInContext(vars, record);

    vars.addContextVariable(DataRuleEvaluator.PIPELINE_CONTEXT, new HashMap<>());
    vars.addContextVariable(DataRuleEvaluator.RULE_ID_CONTEXT, "ID");

    ELEval elEval = new ELEvaluator("", ConcreteELDefinitionExtractor.get(), DriftRuleEL.class, AlertInfoEL.class);

    Assert.assertFalse(elEval.eval(vars, "${drift:size('/', true)}", Boolean.TYPE));
    Assert.assertEquals("", elEval.eval(vars, "${alert:info()}", String.class));

    record.get().getValueAsList().add(Field.create("foo"));

    Assert.assertTrue(elEval.eval(vars, "${drift:size('/', true)}", Boolean.TYPE));
    Assert.assertNotEquals("", elEval.eval(vars, "${alert:info()}", String.class));
  }

  @Test
  public void testDriftNames() throws Exception {
    ELVars vars = new ELVariables();

    Record record = new RecordImpl("creator", "id", null, null);
    record.set(Field.create(new HashMap<String, Field>()));

    RecordEL.setRecordInContext(vars, record);

    vars.addContextVariable(DataRuleEvaluator.PIPELINE_CONTEXT, new HashMap<>());
    vars.addContextVariable(DataRuleEvaluator.RULE_ID_CONTEXT, "ID");

    ELEval elEval = new ELEvaluator("", ConcreteELDefinitionExtractor.get(), DriftRuleEL.class, AlertInfoEL.class);

    Assert.assertFalse(elEval.eval(vars, "${drift:names('/', true)}", Boolean.TYPE));
    Assert.assertEquals("", elEval.eval(vars, "${alert:info()}", String.class));

    record.get().getValueAsMap().put("foo", Field.create("bar"));

    Assert.assertTrue(elEval.eval(vars, "${drift:names('/', true)}", Boolean.TYPE));
    Assert.assertNotEquals("", elEval.eval(vars, "${alert:info()}", String.class));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDriftNamesIncompatibleTypes() throws Exception {
    ELVars vars = new ELVariables();

    Record record = new RecordImpl("creator", "id", null, null);
    record.set(Field.create(1));

    RecordEL.setRecordInContext(vars, record);

    vars.addContextVariable(DataRuleEvaluator.PIPELINE_CONTEXT, new HashMap<>());
    vars.addContextVariable(DataRuleEvaluator.RULE_ID_CONTEXT, "ID");

    ELEval elEval = new ELEvaluator("", ConcreteELDefinitionExtractor.get(), DriftRuleEL.class, AlertInfoEL.class);

    Assert.assertFalse(elEval.eval(vars, "${drift:names('/', true)}", Boolean.TYPE));
    Assert.assertThat(elEval.eval(vars, "${alert:info()}", String.class), JUnitMatchers.containsString("Field / have unsupported type of INTEGER."));
  }

  @Test
  public void testDriftOrder() throws Exception {
    ELVars vars = new ELVariables();

    Record record = new RecordImpl("creator", "id", null, null);
    record.set(Field.createListMap(new LinkedHashMap<String, Field>()));
    record.get().getValueAsMap().put("foo", Field.create("FOO"));
    record.get().getValueAsMap().put("bar", Field.create("BAR"));

    RecordEL.setRecordInContext(vars, record);

    vars.addContextVariable(DataRuleEvaluator.PIPELINE_CONTEXT, new HashMap<>());
    vars.addContextVariable(DataRuleEvaluator.RULE_ID_CONTEXT, "ID");

    ELEval elEval = new ELEvaluator("", ConcreteELDefinitionExtractor.get(), DriftRuleEL.class, AlertInfoEL.class);

    Assert.assertFalse(elEval.eval(vars, "${drift:order('/', true)}", Boolean.TYPE));
    Assert.assertEquals("", elEval.eval(vars, "${alert:info()}", String.class));

    record.get().getValueAsListMap().clear();
    record.get().getValueAsMap().put("bar", Field.create("BAR"));
    record.get().getValueAsMap().put("foo", Field.create("FOO"));

    Assert.assertTrue(elEval.eval(vars, "${drift:order('/', true)}", Boolean.TYPE));
    Assert.assertNotEquals("", elEval.eval(vars, "${alert:info()}", String.class));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDriftOrderIncompatibleTypes() throws Exception {
    ELVars vars = new ELVariables();

    Record record = new RecordImpl("creator", "id", null, null);
    record.set(Field.create(1));

    RecordEL.setRecordInContext(vars, record);

    vars.addContextVariable(DataRuleEvaluator.PIPELINE_CONTEXT, new HashMap<>());
    vars.addContextVariable(DataRuleEvaluator.RULE_ID_CONTEXT, "ID");

    ELEval elEval = new ELEvaluator("", ConcreteELDefinitionExtractor.get(), DriftRuleEL.class, AlertInfoEL.class);

    Assert.assertFalse(elEval.eval(vars, "${drift:order('/', true)}", Boolean.TYPE));
    Assert.assertThat(elEval.eval(vars, "${alert:info()}", String.class), JUnitMatchers.containsString("Field / have unsupported type of INTEGER."));
  }

  @Test
  public void testDriftType() throws Exception {
    ELVars vars = new ELVariables();

    Record record = new RecordImpl("creator", "id", null, null);
    record.set(Field.createListMap(new LinkedHashMap<String, Field>()));

    RecordEL.setRecordInContext(vars, record);

    vars.addContextVariable(DataRuleEvaluator.PIPELINE_CONTEXT, new HashMap<>());
    vars.addContextVariable(DataRuleEvaluator.RULE_ID_CONTEXT, "ID");

    ELEval elEval = new ELEvaluator("", ConcreteELDefinitionExtractor.get(), DriftRuleEL.class, AlertInfoEL.class);

    Assert.assertFalse(elEval.eval(vars, "${drift:type('/', true)}", Boolean.TYPE));
    Assert.assertEquals("", elEval.eval(vars, "${alert:info()}", String.class));

    record.set(Field.create("Hello"));

    Assert.assertTrue(elEval.eval(vars, "${drift:type('/', true)}", Boolean.TYPE));
    Assert.assertNotEquals("", elEval.eval(vars, "${alert:info()}", String.class));
  }

}
