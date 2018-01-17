/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.fieldvaluereplacer;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFieldValueReplacerFieldExpressions {

  @Test
  public void testFieldToReplaceIfNull() throws Exception {
    FieldValueReplacerConfig stringFieldReplacement = new FieldValueReplacerConfig();
    stringFieldReplacement.fields = ImmutableList.of("/*[${f:type() == 'STRING'}]");
    stringFieldReplacement.newValue = "defaultValue";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("nullReplacerConditionalConfigs", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
      .addConfiguration("fieldsToConditionallyReplace", null)
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a")
      .build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("a", Field.create(Field.Type.STRING, null));
      map.put("b", Field.create(Field.Type.STRING, "Value"));
      map.put("c", Field.create(Field.Type.STRING, null));

      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      assertEquals(1, output.getRecords().get("a").size());

      record = output.getRecords().get("a").get(0);
      assertTrue(record.has("/a"));
      assertEquals("defaultValue", record.get("/a").getValueAsString());
      assertTrue(record.has("/b"));
      assertEquals("Value", record.get("/b").getValueAsString());
      assertTrue(record.has("/c"));
      assertEquals("defaultValue", record.get("/c").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNullReplacerConditional() throws Exception {
    NullReplacerConditionalConfig replacement = new NullReplacerConditionalConfig();
    replacement.fieldsToNull = ImmutableList.of("/*[${f:type() == 'STRING'}]");

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("nullReplacerConditionalConfigs", ImmutableList.of(replacement))
      .addConfiguration("fieldsToReplaceIfNull", null)
      .addConfiguration("fieldsToConditionallyReplace", null)
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a")
      .build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("a", Field.create(Field.Type.STRING, "Something"));
      map.put("b", Field.create(Field.Type.STRING, "Nothing"));
      map.put("c", Field.create(Field.Type.STRING, null));

      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      assertEquals(1, output.getRecords().get("a").size());

      record = output.getRecords().get("a").get(0);
      assertTrue(record.has("/a"));
      assertEquals(null, record.get("/a").getValueAsString());
      assertTrue(record.has("/b"));
      assertEquals(null, record.get("/b").getValueAsString());
      assertTrue(record.has("/c"));
      assertEquals(null, record.get("/c").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testConditionalReplacementWithValue() throws Exception {
    FieldValueConditionalReplacerConfig replacement = new FieldValueConditionalReplacerConfig();
    replacement.fieldNames = ImmutableList.of("/*[${f:type() == 'STRING'}]");
    replacement.comparisonValue = "Content";
    replacement.operator = "EQUALS";
    replacement.replacementValue = "Yes!";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("nullReplacerConditionalConfigs", null)
      .addConfiguration("fieldsToReplaceIfNull", null)
      .addConfiguration("fieldsToConditionallyReplace", ImmutableList.of(replacement))
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a")
      .build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("a", Field.create(Field.Type.STRING, "Content"));
      map.put("b", Field.create(Field.Type.STRING, "Nothing"));
      map.put("c", Field.create(Field.Type.STRING, "Content"));

      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      assertEquals(1, output.getRecords().get("a").size());

      record = output.getRecords().get("a").get(0);
      assertTrue(record.has("/a"));
      assertEquals("Yes!", record.get("/a").getValueAsString());
      assertTrue(record.has("/b"));
      assertEquals("Nothing", record.get("/b").getValueAsString());
      assertTrue(record.has("/c"));
      assertEquals("Yes!", record.get("/c").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }
}
