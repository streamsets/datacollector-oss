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

package com.streamsets.pipeline.stage.processor.parser;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.parser.net.netflow.NetflowTestUtil;
import com.streamsets.pipeline.lib.parser.net.syslog.SyslogMessage;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static com.streamsets.testing.Matchers.fieldWithValue;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestDataParserProcessor {

  @Test
  public void testSyslogParsing() throws Exception {
    int priority = 17;
    int facility = priority / 8;
    int severity = priority % 8;
    LocalDateTime date = LocalDateTime.now().withNano(0);
    String host = "1.2.3.4";
    String rest = "Nothing interesting happened.";

    String inputFieldPath = "input";
    String outputFieldPath = "/output";

    String syslogMsg = String.format(
        "<%d>%s %s %s",
        priority,
        DateTimeFormatter.ofPattern("MMM dd HH:mm:ss").format(date),
        host,
        rest
    );

    DataParserConfig configs = new DataParserConfig();
    configs.dataFormat = DataFormat.SYSLOG;
    final DataParserFormatConfig dataParserFormatConfig = new DataParserFormatConfig();
    configs.dataFormatConfig = dataParserFormatConfig;
    configs.fieldPathToParse = "/" + inputFieldPath;
    configs.parsedFieldPath = outputFieldPath;

    DataParserProcessor processor = new DataParserProcessor(configs);

    final String outputLane = "out";

    ProcessorRunner runner = new ProcessorRunner.Builder(DataParserDProcessor.class, processor)
        .addOutputLane(outputLane).setOnRecordError(OnRecordError.TO_ERROR).build();
    Map<String, Field> map = new HashMap<>();
    map.put(inputFieldPath, Field.create(syslogMsg));
    Record record = RecordCreator.create();
    record.set(Field.create(map));
    List<Record> input = new ArrayList<>();
    input.add(record);
    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(input);
      assertTrue(output.getRecords().containsKey(outputLane));
      final List<Record> records = output.getRecords().get(outputLane);
      assertEquals(1, records.size());
      assertTrue(records.get(0).has(outputFieldPath));
      assertEquals(Field.Type.MAP, records.get(0).get(outputFieldPath).getType());
      Map<String, Field> syslogFields = records.get(0).get(outputFieldPath).getValueAsMap();
      assertThat(syslogFields, hasKey(SyslogMessage.FIELD_SYSLOG_PRIORITY));
      assertThat(syslogFields.get(SyslogMessage.FIELD_SYSLOG_PRIORITY), fieldWithValue(priority));
      assertThat(syslogFields.get(SyslogMessage.FIELD_SYSLOG_FACILITY), fieldWithValue(facility));
      assertThat(syslogFields.get(SyslogMessage.FIELD_SYSLOG_SEVERITY), fieldWithValue(severity));
      assertThat(syslogFields.get(SyslogMessage.FIELD_HOST), fieldWithValue(host));
      assertThat(syslogFields.get(SyslogMessage.FIELD_REMAINING), fieldWithValue(rest));
      assertThat(syslogFields.get(SyslogMessage.FIELD_RAW), fieldWithValue(syslogMsg));
      assertThat(syslogFields.get(SyslogMessage.FIELD_TIMESTAMP), fieldWithValue(
          date.toInstant(ZoneOffset.UTC).toEpochMilli()
      ));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNetflowParsing() throws Exception {
    String inputFieldPath = "input";
    String outputFieldPath = "output";

    DataParserConfig configs = new DataParserConfig();
    configs.dataFormat = DataFormat.NETFLOW;
    final DataParserFormatConfig dataParserFormatConfig = new DataParserFormatConfig();
    configs.dataFormatConfig = dataParserFormatConfig;
    configs.fieldPathToParse = "/" + inputFieldPath;
    configs.parsedFieldPath = "/" + outputFieldPath;
    configs.multipleValuesBehavior = MultipleValuesBehavior.SPLIT_INTO_MULTIPLE_RECORDS;

    DataParserProcessor processor = new DataParserProcessor(configs);

    final String outputLane = "out";

    final byte[] netflowBytes = IOUtils.toByteArray(
        TestDataParserProcessor.class.getResourceAsStream("/netflow-v5-file-1"));

    ProcessorRunner runner = new ProcessorRunner.Builder(DataParserDProcessor.class, processor)
        .addOutputLane(outputLane).setOnRecordError(OnRecordError.TO_ERROR).build();
    List<Record> input = new ArrayList<>();

    Map<String, Field> map = new HashMap<>();
    map.put(inputFieldPath, Field.create(netflowBytes));
    Record record = RecordCreator.create();
    record.set(Field.create(map));
    input.add(record);
    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(input);
      assertTrue(output.getRecords().containsKey(outputLane));
      final List<Record> records = output.getRecords().get(outputLane);
      NetflowTestUtil.assertRecordsForTenPackets(records, configs.parsedFieldPath);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testInvalidInputField() throws Exception {
    String inputFieldPath = "input";
    String outputFieldPath = "output";

    DataParserConfig configs = new DataParserConfig();
    configs.dataFormat = DataFormat.JSON;
    final DataParserFormatConfig dataParserFormatConfig = new DataParserFormatConfig();
    configs.dataFormatConfig = dataParserFormatConfig;
    configs.fieldPathToParse = "/" + inputFieldPath + "_non_existent";
    configs.parsedFieldPath = "/" + outputFieldPath;
    configs.multipleValuesBehavior = MultipleValuesBehavior.SPLIT_INTO_MULTIPLE_RECORDS;

    DataParserProcessor processor = new DataParserProcessor(configs);

    final String outputLane = "out";

    ProcessorRunner runner = new ProcessorRunner.Builder(DataParserDProcessor.class, processor)
        .addOutputLane(outputLane).setOnRecordError(OnRecordError.TO_ERROR).build();
    List<Record> input = new ArrayList<>();

    Map<String, Field> map = new HashMap<>();
    map.put(inputFieldPath, Field.create("{\"first\": 1}"));
    Record record = RecordCreator.create();
    record.set(Field.create(map));
    input.add(record);
    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(input);
      assertTrue(output.getRecords().containsKey(outputLane));
      final List<Record> records = output.getRecords().get(outputLane);
      assertThat(records, hasSize(0));
      final List<Record> errors = runner.getErrorRecords();
      assertThat(errors, notNullValue());
      assertThat(errors, hasSize(1));
      Record errorRecord = errors.get(0);
      assertThat(errorRecord.getHeader().getErrorCode(), equalTo(Errors.DATAPARSER_05.name()));
    } finally {
      runner.runDestroy();
    }
  }
}
