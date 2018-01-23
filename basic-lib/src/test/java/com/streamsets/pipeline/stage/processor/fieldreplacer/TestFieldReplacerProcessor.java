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
package com.streamsets.pipeline.stage.processor.fieldreplacer;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.processor.fieldreplacer.config.ReplaceRule;
import com.streamsets.pipeline.stage.processor.fieldreplacer.config.ReplacerConfigBean;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestFieldReplacerProcessor {

  @Test
  public void simpleStaticReplace() throws Exception {
    ReplaceRule rule = new ReplaceRule();
    rule.fields = "/a";
    rule.replacement = "static";
    rule.setToNull = false;

    ProcessorRunner runner = getRunner(rule);

    try {
      Map<String, Field> root = new LinkedHashMap<>();
      root.put("a", Field.create(Field.Type.STRING, "oldValue"));
      root.put("b", Field.create(Field.Type.STRING, "dnt"));
      Record record = RecordCreator.create();
      record.set(Field.create(Field.Type.MAP, root));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      assertEquals(1, output.getRecords().get("lane").size());

      record = output.getRecords().get("lane").get(0);
      assertEquals("static", record.get("/a").getValueAsString());
      assertEquals("dnt", record.get("/b").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void simpleNullReplace() throws Exception {
    ReplaceRule rule = new ReplaceRule();
    rule.fields = "/a";
    rule.setToNull = true;

    ProcessorRunner runner = getRunner(rule);

    try {
      Map<String, Field> root = new LinkedHashMap<>();
      root.put("a", Field.create(Field.Type.STRING, "oldValue"));
      root.put("b", Field.create(Field.Type.STRING, "dnt"));
      Record record = RecordCreator.create();
      record.set(Field.create(Field.Type.MAP, root));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      assertEquals(1, output.getRecords().get("lane").size());

      record = output.getRecords().get("lane").get(0);
      assertEquals(null, record.get("/a").getValueAsString());
      assertEquals("dnt", record.get("/b").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void fieldPathExpressions() throws Exception {
    ReplaceRule rule = new ReplaceRule();
    rule.fields = "/*[${f:type() == 'STRING'}]";
    rule.replacement = "prefix_${f:value()}";
    rule.setToNull = false;

    ProcessorRunner runner = getRunner(rule);

    try {
      Map<String, Field> root = new LinkedHashMap<>();
      root.put("a", Field.create(Field.Type.STRING, "a"));
      root.put("b", Field.create(Field.Type.STRING, "b"));
      root.put("c", Field.create(Field.Type.INTEGER, 1));
      Record record = RecordCreator.create();
      record.set(Field.create(Field.Type.MAP, root));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      assertEquals(1, output.getRecords().get("lane").size());

      record = output.getRecords().get("lane").get(0);
      assertEquals("prefix_a", record.get("/a").getValueAsString());
      assertEquals("prefix_b", record.get("/b").getValueAsString());
      assertEquals(1, record.get("/c").getValueAsInteger());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void fieldDoesNotExistsContinue() throws Exception {
    ReplaceRule rule = new ReplaceRule();
    rule.fields = "/a";
    rule.replacement = "static";

    ProcessorRunner runner = getRunner(OnStagePreConditionFailure.CONTINUE, rule);

    try {
      Record record = RecordCreator.create();
      record.set(Field.create(Field.Type.MAP, new LinkedHashMap<>()));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      assertEquals(1, output.getRecords().get("lane").size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void fieldDoesNotExistsToError() throws Exception {
    ReplaceRule rule = new ReplaceRule();
    rule.fields = "/a";
    rule.replacement = "static";

    ProcessorRunner runner = getRunner(OnStagePreConditionFailure.TO_ERROR, rule);

    try {
      Record record = RecordCreator.create();
      record.set(Field.create(Field.Type.MAP, new LinkedHashMap<>()));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      assertEquals(0, output.getRecords().get("lane").size());
      assertEquals(1, runner.getErrorRecords().size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void fieldExpressionEmptyToError() throws Exception {
    ReplaceRule rule = new ReplaceRule();
    rule.fields = "/*[${f:value() == 'SORRY NOT HERE'}]";
    rule.replacement = "static";

    ProcessorRunner runner = getRunner(OnStagePreConditionFailure.TO_ERROR, rule);

    try {
      Record record = RecordCreator.create();
      record.set(Field.create(Field.Type.MAP, new LinkedHashMap<>()));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      assertEquals(0, output.getRecords().get("lane").size());
      assertEquals(1, runner.getErrorRecords().size());
    } finally {
      runner.runDestroy();
    }
  }

  private static ProcessorRunner getRunner(ReplaceRule ...rules) throws StageException {
    return getRunner(OnStagePreConditionFailure.CONTINUE, rules);
  }

  private static ProcessorRunner getRunner(OnStagePreConditionFailure missingField, ReplaceRule ...rules) throws StageException {
    ReplacerConfigBean config = new ReplacerConfigBean();
    config.rules = ImmutableList.copyOf(rules);
    config.onStagePreConditionFailure = missingField;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldReplacerDProcessor.class, new FieldReplacerProcessor(config))
      .addOutputLane("lane")
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();

    runner.runInit();

    return runner;
  }
}
