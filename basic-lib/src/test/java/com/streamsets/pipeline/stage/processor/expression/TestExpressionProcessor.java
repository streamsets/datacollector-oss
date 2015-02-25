/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestExpressionProcessor {

  @Test
  public void testInvalidExpression() throws StageException {

    ExpressionProcessorConfig expressionProcessorConfig = new ExpressionProcessorConfig();
    expressionProcessorConfig.expression = "${(record:value('baseSalary') +record:value('bonus') * 2}"; //invalid expression string, missing ")"
    expressionProcessorConfig.fieldToSet = "/grossSalary";

    ProcessorRunner runner = new ProcessorRunner.Builder(ExpressionDProcessor.class)
      .addConfiguration("constants", ImmutableMap.of("x-1", "x"))
      .addConfiguration("expressionProcessorConfigs", ImmutableList.of(expressionProcessorConfig))
      .addOutputLane("a").build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(2, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains("EXPR_01"));
    Assert.assertTrue(issues.get(1).toString().contains("EXPR_00"));
  }

  @Test
  public void tesExpressionEvaluationFailure() throws StageException {

    ExpressionProcessorConfig expressionProcessorConfig = new ExpressionProcessorConfig();
    expressionProcessorConfig.expression = "${record:value('/baseSalary') + record:value('/bonusx')}";
    expressionProcessorConfig.fieldToSet = "/grossSalary";

    ProcessorRunner runner = new ProcessorRunner.Builder(ExpressionDProcessor.class)
      .addConfiguration("constants", null)
      .addConfiguration("expressionProcessorConfigs", ImmutableList.of(expressionProcessorConfig))
      .addOutputLane("a").build();

    try {
      runner.runInit();

      Map<String, Field> map = new LinkedHashMap<>();
      map.put("baseSalary", Field.create(Field.Type.STRING, "100000.25"));
      map.put("bonusx", Field.create(Field.Type.STRING, "xxx"));
      map.put("tax", Field.create(Field.Type.STRING, "30000.25"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      runner.runProcess(ImmutableList.of(record));
      Assert.fail("Stage exception expected as the expression string is not valid");
    } catch (OnRecordErrorException e) {
      Assert.assertEquals(Errors.EXPR_03, e.getErrorCode());
    }
  }

  @Test
  public void testReplaceExistingFieldInExpression() throws StageException {

    ExpressionProcessorConfig expressionProcessorConfig = new ExpressionProcessorConfig();
    expressionProcessorConfig.expression = "${record:value('/baseSalary') + record:value('/bonus') - record:value('/tax')}";
    expressionProcessorConfig.fieldToSet = "/baseSalary";

    ProcessorRunner runner = new ProcessorRunner.Builder(ExpressionDProcessor.class)
      .addConfiguration("constants", null)
      .addConfiguration("expressionProcessorConfigs", ImmutableList.of(expressionProcessorConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("baseSalary", Field.create(Field.Type.DOUBLE, 100000.25));
      map.put("bonus", Field.create(Field.Type.INTEGER, 2000));
      map.put("tax", Field.create(Field.Type.DECIMAL, new BigDecimal(30000.25)));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals(3, result.size());
      Assert.assertTrue(result.containsKey("baseSalary"));
      Assert.assertEquals(0, new BigDecimal(100000.25 + 2000 - 30000.25).compareTo((BigDecimal) result.get("baseSalary").getValue()));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testSimpleExpression() throws StageException {

    ExpressionProcessorConfig expressionProcessorConfig = new ExpressionProcessorConfig();
    expressionProcessorConfig.expression = "${record:value('/baseSalary') + record:value('/bonus') - record:value('/tax')}";
    expressionProcessorConfig.fieldToSet = "/netSalary";

    ProcessorRunner runner = new ProcessorRunner.Builder(ExpressionDProcessor.class)
      .addConfiguration("constants", null)
      .addConfiguration("expressionProcessorConfigs", ImmutableList.of(expressionProcessorConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("baseSalary", Field.create(Field.Type.DOUBLE, 100000.25));
      map.put("bonus", Field.create(Field.Type.INTEGER, 2000));
      map.put("tax", Field.create(Field.Type.DECIMAL, new BigDecimal(30000.25)));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals(4, result.size());
      Assert.assertTrue(result.containsKey("netSalary"));
      Assert.assertEquals(0, new BigDecimal(100000.25 + 2000 - 30000.25).compareTo((BigDecimal) result.get("netSalary").getValue()));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testComplexExpression() throws StageException {

    ExpressionProcessorConfig complexExpressionConfig = new ExpressionProcessorConfig();
    complexExpressionConfig.expression = "${((record:value('/baseSalary') * 2) + record:value('/bonus') - (record:value('/perks') / record:value('/bonus')))/2}";
    complexExpressionConfig.fieldToSet = "/complexResult";

    ProcessorRunner runner = new ProcessorRunner.Builder(ExpressionDProcessor.class)
      .addConfiguration("constants", null)
      .addConfiguration("expressionProcessorConfigs", ImmutableList.of(complexExpressionConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("baseSalary", Field.create(Field.Type.DOUBLE, 100000.25));
      map.put("bonus", Field.create(Field.Type.INTEGER, 2000));
      map.put("perks", Field.create(Field.Type.SHORT, 200));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals(4, result.size());
      Assert.assertTrue(result.containsKey("complexResult"));
      Assert.assertEquals(101000.2, result.get("complexResult").getValue()); //((100000.25 * 2) + 2000 - (200 / 2000))/2
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testSubstringExpression() throws StageException {

    ExpressionProcessorConfig expressionProcessorConfig = new ExpressionProcessorConfig();
    expressionProcessorConfig.expression = "${str:substring(record:value('/fullName') , 6, 20)}";
    expressionProcessorConfig.fieldToSet = "/lastName";

    ExpressionProcessorConfig expressionProcessorConfig1 = new ExpressionProcessorConfig();
    expressionProcessorConfig1.expression = "${str:substring(record:value('/fullName') , 10, 20)}";
    expressionProcessorConfig1.fieldToSet = "/empty";

    ExpressionProcessorConfig expressionProcessorConfig2 = new ExpressionProcessorConfig();
    expressionProcessorConfig2.expression = "${str:substring(record:value('/fullName') , 0, 6)}";
    expressionProcessorConfig2.fieldToSet = "/first";

    ProcessorRunner runner = new ProcessorRunner.Builder(ExpressionDProcessor.class)
      .addConfiguration("constants", null)
      .addConfiguration("expressionProcessorConfigs", ImmutableList.of(expressionProcessorConfig, expressionProcessorConfig1, expressionProcessorConfig2))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("fullName", Field.create(Field.Type.STRING, "streamsets"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals(4, result.size());
      Assert.assertTrue(result.containsKey("lastName"));
      Assert.assertEquals("sets", result.get("lastName").getValue());
      Assert.assertTrue(result.containsKey("empty"));
      Assert.assertEquals("", result.get("empty").getValue());
      Assert.assertTrue(result.containsKey("first"));
      Assert.assertEquals("stream", result.get("first").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRecordId() throws StageException {

    ExpressionProcessorConfig complexExpressionConfig = new ExpressionProcessorConfig();
    complexExpressionConfig.expression = "${record:id()}";
    complexExpressionConfig.fieldToSet = "/id";

    ProcessorRunner runner = new ProcessorRunner.Builder(ExpressionDProcessor.class)
        .addConfiguration("constants", null)
        .addConfiguration("expressionProcessorConfigs", ImmutableList.of(complexExpressionConfig))
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals(1, result.size());
      Assert.assertTrue(result.containsKey("id"));
      Assert.assertEquals("s:1", result.get("id").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFailingFieldSet() throws StageException {

    ExpressionProcessorConfig complexExpressionConfig = new ExpressionProcessorConfig();
    complexExpressionConfig.expression = "${record:id()}";
    complexExpressionConfig.fieldToSet = "/id/xx";

    ProcessorRunner runner = new ProcessorRunner.Builder(ExpressionDProcessor.class)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addConfiguration("constants", null)
        .addConfiguration("expressionProcessorConfigs", ImmutableList.of(complexExpressionConfig))
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(0, output.getRecords().get("a").size());
      Assert.assertEquals(1, runner.getErrorRecords().size());
      Assert.assertEquals("s:1", runner.getErrorRecords().get(0).getHeader().getSourceId());
    } finally {
      runner.runDestroy();
    }
  }

}
