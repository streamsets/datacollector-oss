/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.record.RecordImpl;

import java.io.IOException;

public class TestKafkaProcuder {

  private static final String TEST_STRING = "TestFileErrorRecordStore";
  private static final String MIME = "application/octet-stream";

  public static void main(String[] args) throws IOException {
    KafkaProducer p = new KafkaProducer("DD", "0", new KafkaBroker("localhost", 9001), PayloadType.STRING,
      PartitionStrategy.FIXED);
    p.init();

    Record r1 = new RecordImpl("s", "s:1", TEST_STRING.getBytes(), MIME);

    String json = "{\n" +
      "  \"issues\" : {\n" +
      "    \"pipelineIssues\" : [ ],\n" +
      "    \"stageIssues\" : { },\n" +
      "    \"issueCount\" : 0\n" +
      "  },\n" +
      "  \"metrics\" : {\n" +
      "    \"version\" : \"3.0.0\",\n" +
      "    \"gauges\" : { },\n" +
      "    \"counters\" : {\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.errorRecords.counter\" : {\n" +
      "        \"count\" : 0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.inputRecords.counter\" : {\n" +
      "        \"count\" : 9\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.outputRecords.counter\" : {\n" +
      "        \"count\" : 9\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.stageErrors.counter\" : {\n" +
      "        \"count\" : 0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.errorRecords.counter\" : {\n" +
      "        \"count\" : 0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.inputRecords.counter\" : {\n" +
      "        \"count\" : 0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.outputRecords.counter\" : {\n" +
      "        \"count\" : 9\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.stageErrors.counter\" : {\n" +
      "        \"count\" : 0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.errorRecords.counter\" : {\n" +
      "        \"count\" : 0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.inputRecords.counter\" : {\n" +
      "        \"count\" : 0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.outputRecords.counter\" : {\n" +
      "        \"count\" : 0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.stageErrors.counter\" : {\n" +
      "        \"count\" : 0\n" +
      "      }\n" +
      "    },\n" +
      "    \"histograms\" : {\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.errorRecords.histogramM5\" : {\n" +
      "        \"count\" : 1,\n" +
      "        \"max\" : 0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"min\" : 0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"p98\" : 0.0,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"stddev\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.inputRecords.histogramM5\" : {\n" +
      "        \"count\" : 1,\n" +
      "        \"max\" : 9,\n" +
      "        \"mean\" : 9.0,\n" +
      "        \"min\" : 9,\n" +
      "        \"p50\" : 9.0,\n" +
      "        \"p75\" : 9.0,\n" +
      "        \"p95\" : 9.0,\n" +
      "        \"p98\" : 9.0,\n" +
      "        \"p99\" : 9.0,\n" +
      "        \"p999\" : 9.0,\n" +
      "        \"stddev\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.outputRecords.histogramM5\" : {\n" +
      "        \"count\" : 1,\n" +
      "        \"max\" : 9,\n" +
      "        \"mean\" : 9.0,\n" +
      "        \"min\" : 9,\n" +
      "        \"p50\" : 9.0,\n" +
      "        \"p75\" : 9.0,\n" +
      "        \"p95\" : 9.0,\n" +
      "        \"p98\" : 9.0,\n" +
      "        \"p99\" : 9.0,\n" +
      "        \"p999\" : 9.0,\n" +
      "        \"stddev\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.stageErrors.histogramM5\" : {\n" +
      "        \"count\" : 1,\n" +
      "        \"max\" : 0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"min\" : 0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"p98\" : 0.0,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"stddev\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.errorRecords.histogramM5\" : {\n" +
      "        \"count\" : 1,\n" +
      "        \"max\" : 0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"min\" : 0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"p98\" : 0.0,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"stddev\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.inputRecords.histogramM5\" : {\n" +
      "        \"count\" : 1,\n" +
      "        \"max\" : 0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"min\" : 0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"p98\" : 0.0,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"stddev\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.outputRecords.histogramM5\" : {\n" +
      "        \"count\" : 1,\n" +
      "        \"max\" : 9,\n" +
      "        \"mean\" : 9.0,\n" +
      "        \"min\" : 9,\n" +
      "        \"p50\" : 9.0,\n" +
      "        \"p75\" : 9.0,\n" +
      "        \"p95\" : 9.0,\n" +
      "        \"p98\" : 9.0,\n" +
      "        \"p99\" : 9.0,\n" +
      "        \"p999\" : 9.0,\n" +
      "        \"stddev\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.stageErrors.histogramM5\" : {\n" +
      "        \"count\" : 1,\n" +
      "        \"max\" : 0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"min\" : 0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"p98\" : 0.0,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"stddev\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.errorRecords.histogramM5\" : {\n" +
      "        \"count\" : 0,\n" +
      "        \"max\" : 0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"min\" : 0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"p98\" : 0.0,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"stddev\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.inputRecords.histogramM5\" : {\n" +
      "        \"count\" : 0,\n" +
      "        \"max\" : 0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"min\" : 0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"p98\" : 0.0,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"stddev\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.outputRecords.histogramM5\" : {\n" +
      "        \"count\" : 0,\n" +
      "        \"max\" : 0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"min\" : 0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"p98\" : 0.0,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"stddev\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.stageErrors.histogramM5\" : {\n" +
      "        \"count\" : 0,\n" +
      "        \"max\" : 0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"min\" : 0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"p98\" : 0.0,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"stddev\" : 0.0\n" +
      "      }\n" +
      "    },\n" +
      "    \"meters\" : {\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.errorRecords.meter\" : {\n" +
      "        \"count\" : 0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"h6_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\"\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.inputRecords.meter\" : {\n" +
      "        \"count\" : 9,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"h6_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 3275.1091703056773,\n" +
      "        \"units\" : \"events/second\"\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.outputRecords.meter\" : {\n" +
      "        \"count\" : 9,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"h6_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 3266.787658802178,\n" +
      "        \"units\" : \"events/second\"\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.stageErrors.meter\" : {\n" +
      "        \"count\" : 0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"h6_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\"\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.errorRecords.meter\" : {\n" +
      "        \"count\" : 0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"h6_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\"\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.inputRecords.meter\" : {\n" +
      "        \"count\" : 0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"h6_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\"\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.outputRecords.meter\" : {\n" +
      "        \"count\" : 9,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"h6_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 3167.8986272439283,\n" +
      "        \"units\" : \"events/second\"\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.stageErrors.meter\" : {\n" +
      "        \"count\" : 0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"h6_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\"\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.errorRecords.meter\" : {\n" +
      "        \"count\" : 0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"h6_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\"\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.inputRecords.meter\" : {\n" +
      "        \"count\" : 0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"h6_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\"\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.outputRecords.meter\" : {\n" +
      "        \"count\" : 0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"h6_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\"\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.stageErrors.meter\" : {\n" +
      "        \"count\" : 0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"h6_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\"\n" +
      "      }\n" +
      "    },\n" +
      "    \"timers\" : {\n" +
      "      \"pipeline.batchProcessing.timer\" : {\n" +
      "        \"count\" : 1,\n" +
      "        \"max\" : 0.001,\n" +
      "        \"mean\" : 0.001,\n" +
      "        \"min\" : 0.001,\n" +
      "        \"p50\" : 0.001,\n" +
      "        \"p75\" : 0.001,\n" +
      "        \"p95\" : 0.001,\n" +
      "        \"p98\" : 0.001,\n" +
      "        \"p99\" : 0.001,\n" +
      "        \"p999\" : 0.001,\n" +
      "        \"stddev\" : 0.0,\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 170.96939647803043,\n" +
      "        \"duration_units\" : \"seconds\",\n" +
      "        \"rate_units\" : \"calls/second\"\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.batchProcessing.timer\" : {\n" +
      "        \"count\" : 1,\n" +
      "        \"max\" : 0.0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"min\" : 0.0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"p98\" : 0.0,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"stddev\" : 0.0,\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 326.47730982696703,\n" +
      "        \"duration_units\" : \"seconds\",\n" +
      "        \"rate_units\" : \"calls/second\"\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.batchProcessing.timer\" : {\n" +
      "        \"count\" : 1,\n" +
      "        \"max\" : 0.001,\n" +
      "        \"mean\" : 0.001,\n" +
      "        \"min\" : 0.001,\n" +
      "        \"p50\" : 0.001,\n" +
      "        \"p75\" : 0.001,\n" +
      "        \"p95\" : 0.001,\n" +
      "        \"p98\" : 0.001,\n" +
      "        \"p99\" : 0.001,\n" +
      "        \"p999\" : 0.001,\n" +
      "        \"stddev\" : 0.0,\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 313.77470975839344,\n" +
      "        \"duration_units\" : \"seconds\",\n" +
      "        \"rate_units\" : \"calls/second\"\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.batchProcessing.timer\" : {\n" +
      "        \"count\" : 0,\n" +
      "        \"max\" : 0.0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"min\" : 0.0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"p98\" : 0.0,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"stddev\" : 0.0,\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"duration_units\" : \"seconds\",\n" +
      "        \"rate_units\" : \"calls/second\"\n" +
      "      }\n" +
      "    }\n" +
      "  },\n" +
      "  \"batchesOutput\" : [ [ {\n" +
      "    \"instanceName\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "    \"output\" : {\n" +
      "      \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836OutputLane\" : [ {\n" +
      "        \"header\" : {\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.1\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"trackingId\" : \"e86a1e06-5ff2-4947-986e-1c21febb00e1\",\n" +
      "          \"previousTrackingId\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"errorStage\" : null,\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"values\" : { }\n" +
      "        },\n" +
      "        \"value\" : {\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\",\n" +
      "          \"value\" : \"Hello World1\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"header\" : {\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.2\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"trackingId\" : \"32e50447-37d9-418a-95e0-988dccbbf881\",\n" +
      "          \"previousTrackingId\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"errorStage\" : null,\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"values\" : { }\n" +
      "        },\n" +
      "        \"value\" : {\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\",\n" +
      "          \"value\" : \"Hello World2\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"header\" : {\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.3\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"trackingId\" : \"bb0999df-c5b0-4efe-93f4-0138ab777f49\",\n" +
      "          \"previousTrackingId\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"errorStage\" : null,\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"values\" : { }\n" +
      "        },\n" +
      "        \"value\" : {\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\",\n" +
      "          \"value\" : \"Hello World3\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"header\" : {\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.4\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"trackingId\" : \"aa1e2125-9b66-4d4d-bcda-4bc1a8124ddc\",\n" +
      "          \"previousTrackingId\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"errorStage\" : null,\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"values\" : { }\n" +
      "        },\n" +
      "        \"value\" : {\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\",\n" +
      "          \"value\" : \"{name=Field[LONG:-7429029208729569004],  age=Field[LONG:-602533880068081207],  address=Field[LONG:-8955127318593011568]}\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"header\" : {\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.5\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"trackingId\" : \"96b92f93-51f4-4296-82e7-bc06c8ec041b\",\n" +
      "          \"previousTrackingId\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"errorStage\" : null,\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"values\" : { }\n" +
      "        },\n" +
      "        \"value\" : {\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\",\n" +
      "          \"value\" : \"{name=Field[LONG:187122823383125379],  age=Field[LONG:-662007430874906746],  address=Field[LONG:7066511828310424980]}\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"header\" : {\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.6\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"trackingId\" : \"63b853f9-7670-477d-8b00-65c738353dce\",\n" +
      "          \"previousTrackingId\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"errorStage\" : null,\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"values\" : { }\n" +
      "        },\n" +
      "        \"value\" : {\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\",\n" +
      "          \"value\" : \"{name=Field[LONG:-1709849925749487476],  age=Field[LONG:6363375225610636316],  address=Field[LONG:8522646731405058366]}\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"header\" : {\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.7\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"trackingId\" : \"887a3b28-fc6d-4516-bf1d-156674a03807\",\n" +
      "          \"previousTrackingId\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"errorStage\" : null,\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"values\" : { }\n" +
      "        },\n" +
      "        \"value\" : {\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\",\n" +
      "          \"value\" : \"{name=Field[LONG:2851096269902188681],  age=Field[LONG:3331803481733252603],  address=Field[LONG:-3861190799665272555]}\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"header\" : {\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.8\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"trackingId\" : \"4c82b792-c807-4582-8e4c-2ce1298e0703\",\n" +
      "          \"previousTrackingId\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"errorStage\" : null,\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"values\" : { }\n" +
      "        },\n" +
      "        \"value\" : {\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\",\n" +
      "          \"value\" : \"{name=Field[LONG:4245986050541695170],  age=Field[LONG:7431308974704183493],  address=Field[LONG:-3115681965259175738]}\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"header\" : {\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.9\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"trackingId\" : \"4f9635d1-a983-46cb-b584-0a4301b92b8c\",\n" +
      "          \"previousTrackingId\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"errorStage\" : null,\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"values\" : { }\n" +
      "        },\n" +
      "        \"value\" : {\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\",\n" +
      "          \"value\" : \"{name=Field[LONG:-9189719844115460952],  age=Field[LONG:1834903049241238530],  address=Field[LONG:7400946858067887324]}\"\n" +
      "        }\n" +
      "      } ]\n" +
      "    },\n" +
      "    \"errorRecords\" : [ ],\n" +
      "    \"stageErrors\" : null\n" +
      "  }, {\n" +
      "    \"instanceName\" : \"com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "    \"output\" : {\n" +
      "      \"com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797OutputLane\" : [ {\n" +
      "        \"header\" : {\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.1\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836:com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "          \"trackingId\" : \"14f12181-541b-41b4-846c-d0dc44e1b2c8\",\n" +
      "          \"previousTrackingId\" : \"e86a1e06-5ff2-4947-986e-1c21febb00e1\",\n" +
      "          \"raw\" : null,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"errorStage\" : null,\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"values\" : { }\n" +
      "        },\n" +
      "        \"value\" : {\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\",\n" +
      "          \"value\" : \"Hello World1\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"header\" : {\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.2\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836:com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "          \"trackingId\" : \"90e23ee6-6a35-44f6-80d4-ab780813967f\",\n" +
      "          \"previousTrackingId\" : \"32e50447-37d9-418a-95e0-988dccbbf881\",\n" +
      "          \"raw\" : null,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"errorStage\" : null,\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"values\" : { }\n" +
      "        },\n" +
      "        \"value\" : {\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\",\n" +
      "          \"value\" : \"Hello World2\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"header\" : {\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.3\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836:com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "          \"trackingId\" : \"a8ba15eb-7cbd-4463-b549-3f12fa8edd42\",\n" +
      "          \"previousTrackingId\" : \"bb0999df-c5b0-4efe-93f4-0138ab777f49\",\n" +
      "          \"raw\" : null,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"errorStage\" : null,\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"values\" : { }\n" +
      "        },\n" +
      "        \"value\" : {\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\",\n" +
      "          \"value\" : \"Hello World3\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"header\" : {\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.4\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836:com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "          \"trackingId\" : \"e0fec7a7-ffc5-451b-a235-7bd84f34713c\",\n" +
      "          \"previousTrackingId\" : \"aa1e2125-9b66-4d4d-bcda-4bc1a8124ddc\",\n" +
      "          \"raw\" : null,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"errorStage\" : null,\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"values\" : { }\n" +
      "        },\n" +
      "        \"value\" : {\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\",\n" +
      "          \"value\" : \"{name=Field[LONG:-7429029208729569004],  age=Field[LONG:-602533880068081207],  address=Field[LONG:-8955127318593011568]}\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"header\" : {\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.5\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836:com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "          \"trackingId\" : \"c031a1a8-7f47-473b-9dfb-87a389d1ae4f\",\n" +
      "          \"previousTrackingId\" : \"96b92f93-51f4-4296-82e7-bc06c8ec041b\",\n" +
      "          \"raw\" : null,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"errorStage\" : null,\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"values\" : { }\n" +
      "        },\n" +
      "        \"value\" : {\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\",\n" +
      "          \"value\" : \"{name=Field[LONG:187122823383125379],  age=Field[LONG:-662007430874906746],  address=Field[LONG:7066511828310424980]}\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"header\" : {\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.6\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836:com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "          \"trackingId\" : \"eabef4ef-8d34-4fad-bcca-aa8021bfbcbf\",\n" +
      "          \"previousTrackingId\" : \"63b853f9-7670-477d-8b00-65c738353dce\",\n" +
      "          \"raw\" : null,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"errorStage\" : null,\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"values\" : { }\n" +
      "        },\n" +
      "        \"value\" : {\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\",\n" +
      "          \"value\" : \"{name=Field[LONG:-1709849925749487476],  age=Field[LONG:6363375225610636316],  address=Field[LONG:8522646731405058366]}\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"header\" : {\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.7\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836:com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "          \"trackingId\" : \"a28beffc-9282-4d6e-a592-4fc5810250c3\",\n" +
      "          \"previousTrackingId\" : \"887a3b28-fc6d-4516-bf1d-156674a03807\",\n" +
      "          \"raw\" : null,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"errorStage\" : null,\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"values\" : { }\n" +
      "        },\n" +
      "        \"value\" : {\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\",\n" +
      "          \"value\" : \"{name=Field[LONG:2851096269902188681],  age=Field[LONG:3331803481733252603],  address=Field[LONG:-3861190799665272555]}\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"header\" : {\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.8\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836:com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "          \"trackingId\" : \"8ef50cdb-6901-4819-940a-97c31bbdbcfa\",\n" +
      "          \"previousTrackingId\" : \"4c82b792-c807-4582-8e4c-2ce1298e0703\",\n" +
      "          \"raw\" : null,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"errorStage\" : null,\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"values\" : { }\n" +
      "        },\n" +
      "        \"value\" : {\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\",\n" +
      "          \"value\" : \"{name=Field[LONG:4245986050541695170],  age=Field[LONG:7431308974704183493],  address=Field[LONG:-3115681965259175738]}\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"header\" : {\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.9\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836:com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "          \"trackingId\" : \"a225946a-438a-43ac-bdd3-427456346c78\",\n" +
      "          \"previousTrackingId\" : \"4f9635d1-a983-46cb-b584-0a4301b92b8c\",\n" +
      "          \"raw\" : null,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"errorStage\" : null,\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"values\" : { }\n" +
      "        },\n" +
      "        \"value\" : {\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\",\n" +
      "          \"value\" : \"{name=Field[LONG:-9189719844115460952],  age=Field[LONG:1834903049241238530],  address=Field[LONG:7400946858067887324]}\"\n" +
      "        }\n" +
      "      } ]\n" +
      "    },\n" +
      "    \"errorRecords\" : [ ],\n" +
      "    \"stageErrors\" : null\n" +
      "  } ] ],\n" +
      "  \"sourceOffset\" : \"0\",\n" +
      "  \"newSourceOffset\" : \"9\"\n" +
      "}";

    String actual = "{\n" +
      "  \"metrics\" : {\n" +
      "    \"timers\" : {\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.batchProcessing.timer\" : {\n" +
      "        \"min\" : 0.001,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"max\" : 0.001,\n" +
      "        \"count\" : 1,\n" +
      "        \"p999\" : 0.001,\n" +
      "        \"p99\" : 0.001,\n" +
      "        \"mean_rate\" : 313.77470975839344,\n" +
      "        \"duration_units\" : \"seconds\",\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"p50\" : 0.001,\n" +
      "        \"p75\" : 0.001,\n" +
      "        \"rate_units\" : \"calls/second\",\n" +
      "        \"p95\" : 0.001,\n" +
      "        \"mean\" : 0.001,\n" +
      "        \"p98\" : 0.001,\n" +
      "        \"stddev\" : 0.0\n" +
      "      },\n" +
      "      \"pipeline.batchProcessing.timer\" : {\n" +
      "        \"min\" : 0.001,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"max\" : 0.001,\n" +
      "        \"count\" : 1,\n" +
      "        \"p999\" : 0.001,\n" +
      "        \"p99\" : 0.001,\n" +
      "        \"mean_rate\" : 170.96939647803043,\n" +
      "        \"duration_units\" : \"seconds\",\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"p50\" : 0.001,\n" +
      "        \"p75\" : 0.001,\n" +
      "        \"rate_units\" : \"calls/second\",\n" +
      "        \"p95\" : 0.001,\n" +
      "        \"mean\" : 0.001,\n" +
      "        \"p98\" : 0.001,\n" +
      "        \"stddev\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.batchProcessing.timer\" : {\n" +
      "        \"min\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"max\" : 0.0,\n" +
      "        \"count\" : 1,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"mean_rate\" : 326.47730982696703,\n" +
      "        \"duration_units\" : \"seconds\",\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"rate_units\" : \"calls/second\",\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"p98\" : 0.0,\n" +
      "        \"stddev\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.batchProcessing.timer\" : {\n" +
      "        \"min\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"max\" : 0.0,\n" +
      "        \"count\" : 0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"duration_units\" : \"seconds\",\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"rate_units\" : \"calls/second\",\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"p98\" : 0.0,\n" +
      "        \"stddev\" : 0.0\n" +
      "      }\n" +
      "    },\n" +
      "    \"meters\" : {\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.errorRecords.meter\" : {\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"count\" : 0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\",\n" +
      "        \"h6_rate\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.inputRecords.meter\" : {\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"count\" : 9,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 3275.1091703056773,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\",\n" +
      "        \"h6_rate\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.errorRecords.meter\" : {\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"count\" : 0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\",\n" +
      "        \"h6_rate\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.errorRecords.meter\" : {\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"count\" : 0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\",\n" +
      "        \"h6_rate\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.outputRecords.meter\" : {\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"count\" : 0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\",\n" +
      "        \"h6_rate\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.inputRecords.meter\" : {\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"count\" : 0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\",\n" +
      "        \"h6_rate\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.outputRecords.meter\" : {\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"count\" : 9,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 3266.787658802178,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\",\n" +
      "        \"h6_rate\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.stageErrors.meter\" : {\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"count\" : 0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\",\n" +
      "        \"h6_rate\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.outputRecords.meter\" : {\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"count\" : 9,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 3167.8986272439283,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\",\n" +
      "        \"h6_rate\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.inputRecords.meter\" : {\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"count\" : 0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\",\n" +
      "        \"h6_rate\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.stageErrors.meter\" : {\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"count\" : 0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\",\n" +
      "        \"h6_rate\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.stageErrors.meter\" : {\n" +
      "        \"m15_rate\" : 0.0,\n" +
      "        \"h12_rate\" : 0.0,\n" +
      "        \"m5_rate\" : 0.0,\n" +
      "        \"m1_rate\" : 0.0,\n" +
      "        \"count\" : 0,\n" +
      "        \"h24_rate\" : 0.0,\n" +
      "        \"mean_rate\" : 0.0,\n" +
      "        \"h1_rate\" : 0.0,\n" +
      "        \"m30_rate\" : 0.0,\n" +
      "        \"units\" : \"events/second\",\n" +
      "        \"h6_rate\" : 0.0\n" +
      "      }\n" +
      "    },\n" +
      "    \"histograms\" : {\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.inputRecords.histogramM5\" : {\n" +
      "        \"min\" : 0,\n" +
      "        \"max\" : 0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"count\" : 1,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"stddev\" : 0.0,\n" +
      "        \"p98\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.inputRecords.histogramM5\" : {\n" +
      "        \"min\" : 9,\n" +
      "        \"max\" : 9,\n" +
      "        \"p999\" : 9.0,\n" +
      "        \"count\" : 1,\n" +
      "        \"p99\" : 9.0,\n" +
      "        \"p50\" : 9.0,\n" +
      "        \"p75\" : 9.0,\n" +
      "        \"p95\" : 9.0,\n" +
      "        \"mean\" : 9.0,\n" +
      "        \"stddev\" : 0.0,\n" +
      "        \"p98\" : 9.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.errorRecords.histogramM5\" : {\n" +
      "        \"min\" : 0,\n" +
      "        \"max\" : 0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"count\" : 0,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"stddev\" : 0.0,\n" +
      "        \"p98\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.stageErrors.histogramM5\" : {\n" +
      "        \"min\" : 0,\n" +
      "        \"max\" : 0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"count\" : 1,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"stddev\" : 0.0,\n" +
      "        \"p98\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.stageErrors.histogramM5\" : {\n" +
      "        \"min\" : 0,\n" +
      "        \"max\" : 0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"count\" : 1,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"stddev\" : 0.0,\n" +
      "        \"p98\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.errorRecords.histogramM5\" : {\n" +
      "        \"min\" : 0,\n" +
      "        \"max\" : 0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"count\" : 1,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"stddev\" : 0.0,\n" +
      "        \"p98\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.inputRecords.histogramM5\" : {\n" +
      "        \"min\" : 0,\n" +
      "        \"max\" : 0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"count\" : 0,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"stddev\" : 0.0,\n" +
      "        \"p98\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.outputRecords.histogramM5\" : {\n" +
      "        \"min\" : 9,\n" +
      "        \"max\" : 9,\n" +
      "        \"p999\" : 9.0,\n" +
      "        \"count\" : 1,\n" +
      "        \"p99\" : 9.0,\n" +
      "        \"p50\" : 9.0,\n" +
      "        \"p75\" : 9.0,\n" +
      "        \"p95\" : 9.0,\n" +
      "        \"mean\" : 9.0,\n" +
      "        \"stddev\" : 0.0,\n" +
      "        \"p98\" : 9.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.outputRecords.histogramM5\" : {\n" +
      "        \"min\" : 9,\n" +
      "        \"max\" : 9,\n" +
      "        \"p999\" : 9.0,\n" +
      "        \"count\" : 1,\n" +
      "        \"p99\" : 9.0,\n" +
      "        \"p50\" : 9.0,\n" +
      "        \"p75\" : 9.0,\n" +
      "        \"p95\" : 9.0,\n" +
      "        \"mean\" : 9.0,\n" +
      "        \"stddev\" : 0.0,\n" +
      "        \"p98\" : 9.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.stageErrors.histogramM5\" : {\n" +
      "        \"min\" : 0,\n" +
      "        \"max\" : 0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"count\" : 0,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"stddev\" : 0.0,\n" +
      "        \"p98\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.outputRecords.histogramM5\" : {\n" +
      "        \"min\" : 0,\n" +
      "        \"max\" : 0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"count\" : 0,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"stddev\" : 0.0,\n" +
      "        \"p98\" : 0.0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.errorRecords.histogramM5\" : {\n" +
      "        \"min\" : 0,\n" +
      "        \"max\" : 0,\n" +
      "        \"p999\" : 0.0,\n" +
      "        \"count\" : 1,\n" +
      "        \"p99\" : 0.0,\n" +
      "        \"p50\" : 0.0,\n" +
      "        \"p75\" : 0.0,\n" +
      "        \"p95\" : 0.0,\n" +
      "        \"mean\" : 0.0,\n" +
      "        \"stddev\" : 0.0,\n" +
      "        \"p98\" : 0.0\n" +
      "      }\n" +
      "    },\n" +
      "    \"counters\" : {\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.errorRecords.counter\" : {\n" +
      "        \"count\" : 0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.stageErrors.counter\" : {\n" +
      "        \"count\" : 0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.errorRecords.counter\" : {\n" +
      "        \"count\" : 0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.stageErrors.counter\" : {\n" +
      "        \"count\" : 0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.stageErrors.counter\" : {\n" +
      "        \"count\" : 0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.inputRecords.counter\" : {\n" +
      "        \"count\" : 9\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.outputRecords.counter\" : {\n" +
      "        \"count\" : 0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.inputRecords.counter\" : {\n" +
      "        \"count\" : 0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.errorRecords.counter\" : {\n" +
      "        \"count\" : 0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaTarget1418409379279.inputRecords.counter\" : {\n" +
      "        \"count\" : 0\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836.outputRecords.counter\" : {\n" +
      "        \"count\" : 9\n" +
      "      },\n" +
      "      \"stage.com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797.outputRecords.counter\" : {\n" +
      "        \"count\" : 9\n" +
      "      }\n" +
      "    },\n" +
      "    \"gauges\" : { },\n" +
      "    \"version\" : \"3.0.0\"\n" +
      "  },\n" +
      "  \"issues\" : {\n" +
      "    \"issueCount\" : 0,\n" +
      "    \"stageIssues\" : { },\n" +
      "    \"pipelineIssues\" : [ ]\n" +
      "  },\n" +
      "  \"newSourceOffset\" : \"9\",\n" +
      "  \"sourceOffset\" : \"0\",\n" +
      "  \"batchesOutput\" : [ [ {\n" +
      "    \"errorRecords\" : [ ],\n" +
      "    \"instanceName\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "    \"stageErrors\" : null,\n" +
      "    \"output\" : {\n" +
      "      \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836OutputLane\" : [ {\n" +
      "        \"value\" : {\n" +
      "          \"value\" : \"Hello World1\",\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\"\n" +
      "        },\n" +
      "        \"header\" : {\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"values\" : { },\n" +
      "          \"previousTrackingId\" : null,\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"errorStage\" : null,\n" +
      "          \"trackingId\" : \"e86a1e06-5ff2-4947-986e-1c21febb00e1\",\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.1\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"value\" : {\n" +
      "          \"value\" : \"Hello World2\",\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\"\n" +
      "        },\n" +
      "        \"header\" : {\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"values\" : { },\n" +
      "          \"previousTrackingId\" : null,\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"errorStage\" : null,\n" +
      "          \"trackingId\" : \"32e50447-37d9-418a-95e0-988dccbbf881\",\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.2\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"value\" : {\n" +
      "          \"value\" : \"Hello World3\",\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\"\n" +
      "        },\n" +
      "        \"header\" : {\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"values\" : { },\n" +
      "          \"previousTrackingId\" : null,\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"errorStage\" : null,\n" +
      "          \"trackingId\" : \"bb0999df-c5b0-4efe-93f4-0138ab777f49\",\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.3\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"value\" : {\n" +
      "          \"value\" : \"{name=Field[LONG:-7429029208729569004],  age=Field[LONG:-602533880068081207],  address=Field[LONG:-8955127318593011568]}\",\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\"\n" +
      "        },\n" +
      "        \"header\" : {\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"values\" : { },\n" +
      "          \"previousTrackingId\" : null,\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"errorStage\" : null,\n" +
      "          \"trackingId\" : \"aa1e2125-9b66-4d4d-bcda-4bc1a8124ddc\",\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.4\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"value\" : {\n" +
      "          \"value\" : \"{name=Field[LONG:187122823383125379],  age=Field[LONG:-662007430874906746],  address=Field[LONG:7066511828310424980]}\",\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\"\n" +
      "        },\n" +
      "        \"header\" : {\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"values\" : { },\n" +
      "          \"previousTrackingId\" : null,\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"errorStage\" : null,\n" +
      "          \"trackingId\" : \"96b92f93-51f4-4296-82e7-bc06c8ec041b\",\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.5\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"value\" : {\n" +
      "          \"value\" : \"{name=Field[LONG:-1709849925749487476],  age=Field[LONG:6363375225610636316],  address=Field[LONG:8522646731405058366]}\",\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\"\n" +
      "        },\n" +
      "        \"header\" : {\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"values\" : { },\n" +
      "          \"previousTrackingId\" : null,\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"errorStage\" : null,\n" +
      "          \"trackingId\" : \"63b853f9-7670-477d-8b00-65c738353dce\",\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.6\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"value\" : {\n" +
      "          \"value\" : \"{name=Field[LONG:2851096269902188681],  age=Field[LONG:3331803481733252603],  address=Field[LONG:-3861190799665272555]}\",\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\"\n" +
      "        },\n" +
      "        \"header\" : {\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"values\" : { },\n" +
      "          \"previousTrackingId\" : null,\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"errorStage\" : null,\n" +
      "          \"trackingId\" : \"887a3b28-fc6d-4516-bf1d-156674a03807\",\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.7\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"value\" : {\n" +
      "          \"value\" : \"{name=Field[LONG:4245986050541695170],  age=Field[LONG:7431308974704183493],  address=Field[LONG:-3115681965259175738]}\",\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\"\n" +
      "        },\n" +
      "        \"header\" : {\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"values\" : { },\n" +
      "          \"previousTrackingId\" : null,\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"errorStage\" : null,\n" +
      "          \"trackingId\" : \"4c82b792-c807-4582-8e4c-2ce1298e0703\",\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.8\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"value\" : {\n" +
      "          \"value\" : \"{name=Field[LONG:-9189719844115460952],  age=Field[LONG:1834903049241238530],  address=Field[LONG:7400946858067887324]}\",\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\"\n" +
      "        },\n" +
      "        \"header\" : {\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"values\" : { },\n" +
      "          \"previousTrackingId\" : null,\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"errorStage\" : null,\n" +
      "          \"trackingId\" : \"4f9635d1-a983-46cb-b584-0a4301b92b8c\",\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.9\"\n" +
      "        }\n" +
      "      } ]\n" +
      "    }\n" +
      "  }, {\n" +
      "    \"errorRecords\" : [ ],\n" +
      "    \"instanceName\" : \"com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "    \"stageErrors\" : null,\n" +
      "    \"output\" : {\n" +
      "      \"com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797OutputLane\" : [ {\n" +
      "        \"value\" : {\n" +
      "          \"value\" : \"Hello World1\",\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\"\n" +
      "        },\n" +
      "        \"header\" : {\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"values\" : { },\n" +
      "          \"previousTrackingId\" : \"e86a1e06-5ff2-4947-986e-1c21febb00e1\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836:com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "          \"errorStage\" : null,\n" +
      "          \"trackingId\" : \"14f12181-541b-41b4-846c-d0dc44e1b2c8\",\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.1\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"value\" : {\n" +
      "          \"value\" : \"Hello World2\",\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\"\n" +
      "        },\n" +
      "        \"header\" : {\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"values\" : { },\n" +
      "          \"previousTrackingId\" : \"32e50447-37d9-418a-95e0-988dccbbf881\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836:com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "          \"errorStage\" : null,\n" +
      "          \"trackingId\" : \"90e23ee6-6a35-44f6-80d4-ab780813967f\",\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.2\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"value\" : {\n" +
      "          \"value\" : \"Hello World3\",\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\"\n" +
      "        },\n" +
      "        \"header\" : {\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"values\" : { },\n" +
      "          \"previousTrackingId\" : \"bb0999df-c5b0-4efe-93f4-0138ab777f49\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836:com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "          \"errorStage\" : null,\n" +
      "          \"trackingId\" : \"a8ba15eb-7cbd-4463-b549-3f12fa8edd42\",\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.3\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"value\" : {\n" +
      "          \"value\" : \"{name=Field[LONG:-7429029208729569004],  age=Field[LONG:-602533880068081207],  address=Field[LONG:-8955127318593011568]}\",\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\"\n" +
      "        },\n" +
      "        \"header\" : {\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"values\" : { },\n" +
      "          \"previousTrackingId\" : \"aa1e2125-9b66-4d4d-bcda-4bc1a8124ddc\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836:com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "          \"errorStage\" : null,\n" +
      "          \"trackingId\" : \"e0fec7a7-ffc5-451b-a235-7bd84f34713c\",\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.4\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"value\" : {\n" +
      "          \"value\" : \"{name=Field[LONG:187122823383125379],  age=Field[LONG:-662007430874906746],  address=Field[LONG:7066511828310424980]}\",\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\"\n" +
      "        },\n" +
      "        \"header\" : {\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"values\" : { },\n" +
      "          \"previousTrackingId\" : \"96b92f93-51f4-4296-82e7-bc06c8ec041b\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836:com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "          \"errorStage\" : null,\n" +
      "          \"trackingId\" : \"c031a1a8-7f47-473b-9dfb-87a389d1ae4f\",\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.5\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"value\" : {\n" +
      "          \"value\" : \"{name=Field[LONG:-1709849925749487476],  age=Field[LONG:6363375225610636316],  address=Field[LONG:8522646731405058366]}\",\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\"\n" +
      "        },\n" +
      "        \"header\" : {\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"values\" : { },\n" +
      "          \"previousTrackingId\" : \"63b853f9-7670-477d-8b00-65c738353dce\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836:com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "          \"errorStage\" : null,\n" +
      "          \"trackingId\" : \"eabef4ef-8d34-4fad-bcca-aa8021bfbcbf\",\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.6\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"value\" : {\n" +
      "          \"value\" : \"{name=Field[LONG:2851096269902188681],  age=Field[LONG:3331803481733252603],  address=Field[LONG:-3861190799665272555]}\",\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\"\n" +
      "        },\n" +
      "        \"header\" : {\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"values\" : { },\n" +
      "          \"previousTrackingId\" : \"887a3b28-fc6d-4516-bf1d-156674a03807\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836:com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "          \"errorStage\" : null,\n" +
      "          \"trackingId\" : \"a28beffc-9282-4d6e-a592-4fc5810250c3\",\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.7\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"value\" : {\n" +
      "          \"value\" : \"{name=Field[LONG:4245986050541695170],  age=Field[LONG:7431308974704183493],  address=Field[LONG:-3115681965259175738]}\",\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\"\n" +
      "        },\n" +
      "        \"header\" : {\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"values\" : { },\n" +
      "          \"previousTrackingId\" : \"4c82b792-c807-4582-8e4c-2ce1298e0703\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836:com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "          \"errorStage\" : null,\n" +
      "          \"trackingId\" : \"8ef50cdb-6901-4819-940a-97c31bbdbcfa\",\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.8\"\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"value\" : {\n" +
      "          \"value\" : \"{name=Field[LONG:-9189719844115460952],  age=Field[LONG:1834903049241238530],  address=Field[LONG:7400946858067887324]}\",\n" +
      "          \"path\" : \"\",\n" +
      "          \"type\" : \"STRING\"\n" +
      "        },\n" +
      "        \"header\" : {\n" +
      "          \"errorMessage\" : null,\n" +
      "          \"raw\" : null,\n" +
      "          \"stageCreator\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836\",\n" +
      "          \"values\" : { },\n" +
      "          \"previousTrackingId\" : \"4f9635d1-a983-46cb-b584-0a4301b92b8c\",\n" +
      "          \"stagesPath\" : \"com_streamsets_pipeline_lib_stage_source_kafka_KafkaSource1418408917836:com_streamsets_pipeline_lib_stage_processor_nop_IdentityProcessor1418408923797\",\n" +
      "          \"errorStage\" : null,\n" +
      "          \"trackingId\" : \"a225946a-438a-43ac-bdd3-427456346c78\",\n" +
      "          \"errorCode\" : null,\n" +
      "          \"errorTimestamp\" : 0,\n" +
      "          \"rawMimeType\" : null,\n" +
      "          \"sourceId\" : \"BB.1.1418409529445.9\"\n" +
      "        }\n" +
      "      } ]\n" +
      "    }\n" +
      "  } ] ]\n" +
      "}";

    r1.set(Field.create(json));
    ((RecordImpl)r1).getHeader().setTrackingId("t1");
    p.enqueueRecord(r1);
    p.write();
  }
}
