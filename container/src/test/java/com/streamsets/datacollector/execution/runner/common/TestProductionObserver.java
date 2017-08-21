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
package com.streamsets.datacollector.execution.runner.common;

import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.ThresholdType;
import com.streamsets.datacollector.execution.alerts.TestUtil;
import com.streamsets.datacollector.execution.runner.common.ProductionObserver;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.Record;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public class TestProductionObserver {

  private static final String LANE = "lane";
  private static final String ID = "myId";
  private static final int NUMBER_OF_RECORDS_PER_BATCH = 3;
  private static final int NUMBER_OF_BATCHES = 1000;

  private static ProductionObserver productionObserver;

  @Before
  public void setUp() {
    productionObserver = new ProductionObserver(new Configuration(), null);
    productionObserver.setObserveRequests(new ArrayBlockingQueue<>(10));
  }

  @Test
  public void testGetSampledRecords() {
    long timestamp = System.currentTimeMillis();
    List<DataRuleDefinition> dataRuleDefinitions = new ArrayList<>();
    dataRuleDefinitions.add(new DataRuleDefinition(ID+1, "myRule", LANE + "::s", 100 /*Sampling %*/, 5,
      "${record:value(\"/name\")==null}", true, "alertText", ThresholdType.COUNT, "2", 5, true, false, true,
      timestamp));
    dataRuleDefinitions.add(new DataRuleDefinition(ID+2, "myRule", LANE + "::s", 50 /*Sampling %*/, 5,
      "${record:value(\"/name\")==null}", true, "alertText", ThresholdType.COUNT, "2", 5, true, false, true,
      timestamp));
    dataRuleDefinitions.add(new DataRuleDefinition(ID+3, "myRule", LANE + "::s", 25 /*Sampling %*/, 5,
      "${record:value(\"/name\")==null}", true, "alertText", ThresholdType.COUNT, "2", 5, true, false, true,
      timestamp));
    dataRuleDefinitions.add(new DataRuleDefinition(ID+4, "myRule", LANE + "::s", 10 /*Sampling %*/, 5,
      "${record:value(\"/name\")==null}", true, "alertText", ThresholdType.COUNT, "2", 5, true, false, true,
      timestamp));

    //Generates 100 records
    List<Record> allRecords = TestUtil.createRecords(500);

    Map<String, List<Record>> sampleRecords = productionObserver.getSampleRecords(dataRuleDefinitions, allRecords,
      LANE);

    Assert.assertEquals(500, sampleRecords.get(ID + 1).size());
    Assert.assertEquals(250, sampleRecords.get(ID + 2).size());
    Assert.assertEquals(125, sampleRecords.get(ID + 3).size());
    Assert.assertEquals(50, sampleRecords.get(ID + 4).size());
  }

  @Test
  public void testGetSampledRecordsLowThroughput() {

    //To get an idea of how sampling behaves vary the NUMBER_OF_BATCHES and NUMBER_OF_RECORDS_PER_BATCH variables
    //and print out the result by un commenting the following.
    long timestamp = System.currentTimeMillis();
    List<DataRuleDefinition> dataRuleDefinitions = new ArrayList<>();
    dataRuleDefinitions.add(new DataRuleDefinition(ID+1, "myRule", LANE + "::s", 100 /*Sampling %*/, 5,
      "${record:value(\"/name\")==null}", true, "alertText", ThresholdType.COUNT, "2", 5, true, false, true,
      timestamp));
    dataRuleDefinitions.add(new DataRuleDefinition(ID+2, "myRule", LANE + "::s", 50 /*Sampling % */, 5,
      "${record:value(\"/name\")==null}", true, "alertText", ThresholdType.COUNT, "2", 5, true, false, true,
      timestamp));
    dataRuleDefinitions.add(new DataRuleDefinition(ID+3, "myRule", LANE + "::s", 25 /* Sampling % */, 5,
      "${record:value(\"/name\")==null}", true, "alertText", ThresholdType.COUNT, "2", 5, true, false, true,
      timestamp));
    dataRuleDefinitions.add(new DataRuleDefinition(ID+4, "myRule", LANE + "::s", 10 /* Sampling % */, 5,
      "${record:value(\"/name\")==null}", true, "alertText", ThresholdType.COUNT, "2", 5, true, false, true,
      timestamp));
    dataRuleDefinitions.add(new DataRuleDefinition(ID+5, "myRule", LANE + "::s", 5 /* Sampling % */, 5,
      "${record:value(\"/name\")==null}", true, "alertText", ThresholdType.COUNT, "2", 5, true, false, true,
      timestamp));

    //Generates 100 records
    List<Record> allRecords = TestUtil.createRecords(NUMBER_OF_RECORDS_PER_BATCH);

    Map<String, Integer> ruleIdToSampledRecordsSize = new HashMap<>();
    ruleIdToSampledRecordsSize.put(ID + 1, 0);
    ruleIdToSampledRecordsSize.put(ID + 2, 0);
    ruleIdToSampledRecordsSize.put(ID + 3, 0);
    ruleIdToSampledRecordsSize.put(ID + 4, 0);
    ruleIdToSampledRecordsSize.put(ID + 5, 0);

    for(int i = 0; i < NUMBER_OF_BATCHES; i++) {
      Map<String, List<Record>> sampleRecords = productionObserver.getSampleRecords(dataRuleDefinitions, allRecords,
        LANE);
      for(int j = 1; j < 6; j++) {
        if(sampleRecords.containsKey(ID + j)) {
          ruleIdToSampledRecordsSize.put(ID + j,
            ruleIdToSampledRecordsSize.get(ID + j) + sampleRecords.get(ID + j).size());
        }
      }
    }

    Assert.assertEquals(3000, ruleIdToSampledRecordsSize.get(ID + 1).intValue());
    Assert.assertEquals(1500, ruleIdToSampledRecordsSize.get(ID + 2).intValue());
    Assert.assertEquals(750, ruleIdToSampledRecordsSize.get(ID + 3).intValue());
    Assert.assertEquals(300, ruleIdToSampledRecordsSize.get(ID + 4).intValue());
    Assert.assertEquals(150, ruleIdToSampledRecordsSize.get(ID + 5).intValue());

    /*System.out.println("Records for rule myID1 : " + ruleIdToSampledRecordsSize.get(ID + 1));
    System.out.println("Records for rule myID2 : " + ruleIdToSampledRecordsSize.get(ID + 2));
    System.out.println("Records for rule myID3 : " + ruleIdToSampledRecordsSize.get(ID + 3));
    System.out.println("Records for rule myID4 : " + ruleIdToSampledRecordsSize.get(ID + 4));
    System.out.println("Records for rule myID5 : " + ruleIdToSampledRecordsSize.get(ID + 5));*/
  }
}
