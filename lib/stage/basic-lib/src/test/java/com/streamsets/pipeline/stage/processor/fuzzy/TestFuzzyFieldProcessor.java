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
package com.streamsets.pipeline.stage.processor.fuzzy;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Ignore("The stage itself is currently disabled by having the @StageDef annotation commented out on which the tests depends on.")
public class TestFuzzyFieldProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(TestFuzzyFieldProcessor.class);

  @Test
  public void testSimpleMatchInPlace() throws Exception {
    List<String> outputFields = ImmutableList.of("Stream", "Google", "Hadoop");

    ProcessorRunner runner = new ProcessorRunner.Builder(FuzzyFieldDProcessor.class)
        .addConfiguration("rootFieldPaths", ImmutableList.of("/"))
        .addConfiguration("outputFieldNames", outputFields)
        .addConfiguration("matchThreshold", 60)
        .addConfiguration("allCandidates", false)
        .addConfiguration("inPlace", true)
        .addConfiguration("preserveUnmatchedFields", true)
        .addOutputLane("a").build();

    runner.runInit();

    try {
      List<Field> csvWithHeader = new ArrayList<>();
      Map<String, Field> f1 = new HashMap<>();
      f1.put("header", Field.create("Straem"));
      f1.put("value", Field.create("val1"));
      csvWithHeader.add(Field.create(f1));
      Map<String, Field> f2 = new HashMap<>();
      f2.put("header", Field.create("Google"));
      f2.put("value", Field.create("val2"));
      csvWithHeader.add(Field.create(f2));
      Map<String, Field> f3 = new HashMap<>();
      f3.put("header", Field.create("Hadopp"));
      f3.put("value", Field.create("val3"));
      csvWithHeader.add(Field.create(f3));
      Record record = RecordCreator.create("s", "s:1");

      record.set(Field.create(csvWithHeader));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(0, runner.getErrorRecords().size());
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals("val1", result.get("Stream").getValueAsString());
      Assert.assertEquals("val2", result.get("Google").getValueAsString());
      Assert.assertEquals("val3", result.get("Hadoop").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testComplexMatchInPlace() throws Exception {
    List<String> outputFields = ImmutableList.of("Sale Price", "Invoice Price", "Date");

    ProcessorRunner runner = new ProcessorRunner.Builder(FuzzyFieldDProcessor.class)
        .addConfiguration("rootFieldPaths", ImmutableList.of("/"))
        .addConfiguration("outputFieldNames", outputFields)
        .addConfiguration("matchThreshold", 60)
        .addConfiguration("allCandidates", false)
        .addConfiguration("inPlace", true)
        .addConfiguration("preserveUnmatchedFields", true)
        .addOutputLane("a").build();

    runner.runInit();

    try {
      List<Field> csvWithHeader = new ArrayList<>();
      Map<String, Field> f1 = new HashMap<>();
      f1.put("header", Field.create("Inv. Price"));
      f1.put("value", Field.create("100"));
      csvWithHeader.add(Field.create(f1));
      Map<String, Field> f2 = new HashMap<>();
      f2.put("header", Field.create("Sale"));
      f2.put("value", Field.create("1000"));
      csvWithHeader.add(Field.create(f2));
      Map<String, Field> f3 = new HashMap<>();
      f3.put("header", Field.create("Sold Date"));
      f3.put("value", Field.create("2015-01-01"));
      csvWithHeader.add(Field.create(f3));
      Record record = RecordCreator.create("s", "s:1");

      record.set(Field.create(csvWithHeader));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(0, runner.getErrorRecords().size());
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();

      Assert.assertEquals("1000", result.get("Sale Price").getValueAsString());
      Assert.assertEquals("100", result.get("Invoice Price").getValueAsString());
      Assert.assertEquals("2015-01-01", result.get("Date").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testSimpleMatch() throws Exception {
    List<String> outputFields = ImmutableList.of("Stream", "Google", "Hadoop");

    ProcessorRunner runner = new ProcessorRunner.Builder(FuzzyFieldDProcessor.class)
        .addConfiguration("rootFieldPaths", ImmutableList.of("/"))
        .addConfiguration("outputFieldNames", outputFields)
        .addConfiguration("matchThreshold", 60)
        .addConfiguration("allCandidates", true)
        .addConfiguration("inPlace", false)
        .addConfiguration("preserveUnmatchedFields", true)
        .addOutputLane("a").build();

    runner.runInit();

    try {
      List<Field> csvWithHeader = new ArrayList<>();
      Map<String, Field> f1 = new HashMap<>();
      f1.put("header", Field.create("Straem"));
      f1.put("value", Field.create("val1"));
      csvWithHeader.add(Field.create(f1));
      Map<String, Field> f2 = new HashMap<>();
      f2.put("header", Field.create("Google"));
      f2.put("value", Field.create("val2"));
      csvWithHeader.add(Field.create(f2));
      Map<String, Field> f3 = new HashMap<>();
      f3.put("header", Field.create("Hadopp"));
      f3.put("value", Field.create("val3"));
      csvWithHeader.add(Field.create(f3));
      Record record = RecordCreator.create("s", "s:1");

      record.set(Field.create(csvWithHeader));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(0, runner.getErrorRecords().size());
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();

      Assert.assertTrue(result.get("Stream").getValue() instanceof List);
      Assert.assertTrue(result.get("Google").getValue() instanceof List);
      Assert.assertTrue(result.get("Hadoop").getValue() instanceof List);

      List<Field> streamResult = result.get("Stream").getValueAsList();
      Assert.assertEquals(1, streamResult.size());
      Map<String, Field> streamResult0 = streamResult.get(0).getValueAsMap();
      Assert.assertEquals(67, streamResult0.get("score").getValue());
      Assert.assertEquals("Straem", streamResult0.get("header").getValue());
      Assert.assertEquals("val1", streamResult0.get("value").getValue());

      List<Field> googleResult = result.get("Google").getValueAsList();
      Assert.assertEquals(1, googleResult.size());
      Map<String, Field> googleResult0 = googleResult.get(0).getValueAsMap();
      Assert.assertEquals(100, googleResult0.get("score").getValue());
      Assert.assertEquals("Google", googleResult0.get("header").getValue());
      Assert.assertEquals("val2", googleResult0.get("value").getValue());

      List<Field> hadoopResult = result.get("Hadoop").getValueAsList();
      Assert.assertEquals(1, hadoopResult.size());
      Map<String, Field> hadoopResult0 = hadoopResult.get(0).getValueAsMap();
      Assert.assertEquals(84, hadoopResult0.get("score").getValue());
      Assert.assertEquals("Hadopp", hadoopResult0.get("header").getValue());
      Assert.assertEquals("val3", hadoopResult0.get("value").getValue());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testComplexMatch() throws Exception {
    List<String> outputFields = ImmutableList.of("Sale Price", "Invoice Price", "Date");

    ProcessorRunner runner = new ProcessorRunner.Builder(FuzzyFieldDProcessor.class)
        .addConfiguration("rootFieldPaths", ImmutableList.of("/"))
        .addConfiguration("outputFieldNames", outputFields)
        .addConfiguration("matchThreshold", 60)
        .addConfiguration("allCandidates", true)
        .addConfiguration("inPlace", false)
        .addConfiguration("preserveUnmatchedFields", true)
        .addOutputLane("a").build();

    runner.runInit();

    try {
      List<Field> csvWithHeader = new ArrayList<>();
      Map<String, Field> f1 = new HashMap<>();
      f1.put("header", Field.create("Inv. Price"));
      f1.put("value", Field.create("100"));
      csvWithHeader.add(Field.create(f1));
      Map<String, Field> f2 = new HashMap<>();
      f2.put("header", Field.create("Sale"));
      f2.put("value", Field.create("1000"));
      csvWithHeader.add(Field.create(f2));
      Map<String, Field> f3 = new HashMap<>();
      f3.put("header", Field.create("Sold Date"));
      f3.put("value", Field.create("2015-01-01"));
      csvWithHeader.add(Field.create(f3));
      Record record = RecordCreator.create("s", "s:1");

      record.set(Field.create(csvWithHeader));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(0, runner.getErrorRecords().size());
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();

      Assert.assertTrue(result.get("Sale Price").getValue() instanceof List);
      Assert.assertTrue(result.get("Invoice Price").getValue() instanceof List);
      Assert.assertTrue(result.get("Date").getValue() instanceof List);

      List<Field> salePriceResult = result.get("Sale Price").getValueAsList();
      Assert.assertEquals(2, salePriceResult.size());
      Map<String, Field> salePriceResult0= salePriceResult.get(0).getValueAsMap();
      Map<String, Field> salePriceResult1= salePriceResult.get(1).getValueAsMap();

      Assert.assertEquals(60, salePriceResult0.get("score").getValue());
      Assert.assertEquals("Inv. Price", salePriceResult0.get("header").getValue());
      Assert.assertEquals("100", salePriceResult0.get("value").getValue());

      Assert.assertEquals(100, salePriceResult1.get("score").getValue());
      Assert.assertEquals("Sale", salePriceResult1.get("header").getValue());
      Assert.assertEquals("1000", salePriceResult1.get("value").getValue());

      List<Field> invoicePriceResult = result.get("Invoice Price").getValueAsList();
      Assert.assertEquals(1, invoicePriceResult.size());
      Map<String, Field> invoicePriceResult0 = invoicePriceResult.get(0).getValueAsMap();

      Assert.assertEquals(70, invoicePriceResult0.get("score").getValue());
      Assert.assertEquals("Inv. Price", invoicePriceResult0.get("header").getValue());
      Assert.assertEquals("100", invoicePriceResult0.get("value").getValue());

      List<Field> dateResult = result.get("Date").getValueAsList();
      Assert.assertEquals(1, dateResult.size());
      Map<String, Field> dateResult0= dateResult.get(0).getValueAsMap();

      Assert.assertEquals(100, dateResult0.get("score").getValue());
      Assert.assertEquals("Sold Date", dateResult0.get("header").getValue());
      Assert.assertEquals("2015-01-01", dateResult0.get("value").getValue());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testSnakeCaseMatchInPlace() throws Exception {
    List<String> outputFields = ImmutableList.of("sale_price", "invoice_Price", "date");

    ProcessorRunner runner = new ProcessorRunner.Builder(FuzzyFieldDProcessor.class)
        .addConfiguration("rootFieldPaths", ImmutableList.of("/"))
        .addConfiguration("outputFieldNames", outputFields)
        .addConfiguration("matchThreshold", 60)
        .addConfiguration("allCandidates", false)
        .addConfiguration("inPlace", true)
        .addConfiguration("preserveUnmatchedFields", true)
        .addOutputLane("a").build();

    runner.runInit();

    try {
      List<Field> csvWithHeader = new ArrayList<>();
      Map<String, Field> f1 = new HashMap<>();
      f1.put("header", Field.create("Inv. Price"));
      f1.put("value", Field.create("100"));
      csvWithHeader.add(Field.create(f1));
      Map<String, Field> f2 = new HashMap<>();
      f2.put("header", Field.create("Sale"));
      f2.put("value", Field.create("1000"));
      csvWithHeader.add(Field.create(f2));
      Map<String, Field> f3 = new HashMap<>();
      f3.put("header", Field.create("Sold Date"));
      f3.put("value", Field.create("2015-01-01"));
      csvWithHeader.add(Field.create(f3));
      Record record = RecordCreator.create("s", "s:1");

      record.set(Field.create(csvWithHeader));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(0, runner.getErrorRecords().size());
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();

      Assert.assertEquals("1000", result.get("sale_price").getValueAsString());
      Assert.assertEquals("100", result.get("invoice_Price").getValueAsString());
      Assert.assertEquals("2015-01-01", result.get("date").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testCamelCaseMatchInPlace() throws Exception {
    List<String> outputFields = ImmutableList.of("salePrice", "invoicePrice", "date");

    ProcessorRunner runner = new ProcessorRunner.Builder(FuzzyFieldDProcessor.class)
        .addConfiguration("rootFieldPaths", ImmutableList.of("/"))
        .addConfiguration("outputFieldNames", outputFields)
        .addConfiguration("matchThreshold", 60)
        .addConfiguration("allCandidates", false)
        .addConfiguration("inPlace", true)
        .addConfiguration("preserveUnmatchedFields", true)
        .addOutputLane("a").build();

    runner.runInit();

    try {
      List<Field> csvWithHeader = new ArrayList<>();
      Map<String, Field> f1 = new HashMap<>();
      f1.put("header", Field.create("Inv. Price"));
      f1.put("value", Field.create("100"));
      csvWithHeader.add(Field.create(f1));
      Map<String, Field> f2 = new HashMap<>();
      f2.put("header", Field.create("Sale"));
      f2.put("value", Field.create("1000"));
      csvWithHeader.add(Field.create(f2));
      Map<String, Field> f3 = new HashMap<>();
      f3.put("header", Field.create("Sold Date"));
      f3.put("value", Field.create("2015-01-01"));
      csvWithHeader.add(Field.create(f3));
      Record record = RecordCreator.create("s", "s:1");

      record.set(Field.create(csvWithHeader));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(0, runner.getErrorRecords().size());
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();

      Assert.assertEquals("1000", result.get("salePrice").getValueAsString());
      Assert.assertEquals("100", result.get("invoicePrice").getValueAsString());
      Assert.assertEquals("2015-01-01", result.get("date").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMixedCaseMatchInPlace() throws Exception {
    List<String> outputFields = ImmutableList.of("sale_price", "invoice_PriceMsrp", "dateSold");

    ProcessorRunner runner = new ProcessorRunner.Builder(FuzzyFieldDProcessor.class)
        .addConfiguration("rootFieldPaths", ImmutableList.of("/"))
        .addConfiguration("outputFieldNames", outputFields)
        .addConfiguration("matchThreshold", 60)
        .addConfiguration("allCandidates", false)
        .addConfiguration("inPlace", true)
        .addConfiguration("preserveUnmatchedFields", true)
        .addOutputLane("a").build();

    runner.runInit();

    try {
      List<Field> csvWithHeader = new ArrayList<>();
      Map<String, Field> f1 = new HashMap<>();
      f1.put("header", Field.create("Invoice Price"));
      f1.put("value", Field.create("100"));
      csvWithHeader.add(Field.create(f1));
      Map<String, Field> f2 = new HashMap<>();
      f2.put("header", Field.create("Sale"));
      f2.put("value", Field.create("1000"));
      csvWithHeader.add(Field.create(f2));
      Map<String, Field> f3 = new HashMap<>();
      f3.put("header", Field.create("Sold Date"));
      f3.put("value", Field.create("2015-01-01"));
      csvWithHeader.add(Field.create(f3));
      Record record = RecordCreator.create("s", "s:1");

      record.set(Field.create(csvWithHeader));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(0, runner.getErrorRecords().size());
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();

      Assert.assertEquals("1000", result.get("sale_price").getValueAsString());
      Assert.assertEquals("100", result.get("invoice_PriceMsrp").getValueAsString());
      Assert.assertEquals("2015-01-01", result.get("dateSold").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testUnmatchedFieldInPlace() throws Exception {
    List<String> outputFields = ImmutableList.of("sale_price", "invoice_PriceMsrp");

    ProcessorRunner runner = new ProcessorRunner.Builder(FuzzyFieldDProcessor.class)
        .addConfiguration("rootFieldPaths", ImmutableList.of("/"))
        .addConfiguration("outputFieldNames", outputFields)
        .addConfiguration("matchThreshold", 60)
        .addConfiguration("allCandidates", false)
        .addConfiguration("inPlace", true)
        .addConfiguration("preserveUnmatchedFields", true)
        .addOutputLane("a").build();

    runner.runInit();

    try {
      List<Field> csvWithHeader = new ArrayList<>();
      Map<String, Field> f1 = new HashMap<>();
      f1.put("header", Field.create("Invoice Price"));
      f1.put("value", Field.create("100"));
      csvWithHeader.add(Field.create(f1));
      Map<String, Field> f2 = new HashMap<>();
      f2.put("header", Field.create("Sale"));
      f2.put("value", Field.create("1000"));
      csvWithHeader.add(Field.create(f2));
      Map<String, Field> f3 = new HashMap<>();
      f3.put("header", Field.create("Sold Date"));
      f3.put("value", Field.create("2015-01-01"));
      csvWithHeader.add(Field.create(f3));
      Record record = RecordCreator.create("s", "s:1");

      record.set(Field.create(csvWithHeader));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(0, runner.getErrorRecords().size());
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();

      Assert.assertEquals("1000", result.get("sale_price").getValueAsString());
      Assert.assertEquals("100", result.get("invoice_PriceMsrp").getValueAsString());
      Assert.assertEquals("2015-01-01", result.get("Sold Date").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }
}
