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

import com.google.common.io.Resources;
import com.streamsets.datacollector.MiniSDC;
import com.streamsets.datacollector.MiniSDCTestingUtility;
import com.streamsets.datacollector.util.VerifyUtils;
import com.streamsets.pipeline.kafka.common.KafkaTestUtil;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

@Ignore
public class DataRulesIT {

  private static final String TOPIC = "TestKafkaDestinationSinglePartitionPipelineOperations";
  private static final Logger LOG = LoggerFactory.getLogger(DataRulesIT.class);
  private static MiniSDCTestingUtility miniSDCTestingUtility;
  private static URI serverURI;
  private static MiniSDC miniSDC;
  private static String ruleDefinitions;

  @Before
  public void setUp() throws Exception {
    miniSDC.startPipeline();
    VerifyUtils.waitForPipelineToStart(serverURI, getPipelineName(), getPipelineRev());
  }

  @After
  public void tearDown() throws Exception {
    String status = VerifyUtils.getPipelineState(serverURI, getPipelineName(), getPipelineRev());
    if ("RUNNING".equals(status) || "STARTING".equals(status)) {
      miniSDC.stopPipeline();
      VerifyUtils.waitForPipelineToStop(serverURI, getPipelineName(), getPipelineRev());
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    KafkaTestUtil.startZookeeper();
    KafkaTestUtil.startKafkaBrokers(1);
    KafkaTestUtil.createTopic(TOPIC, 1, 1);
    System.setProperty("sdc.testing-mode", "true");
    miniSDCTestingUtility = new MiniSDCTestingUtility();
    miniSDC = miniSDCTestingUtility.createMiniSDC(MiniSDC.ExecutionMode.STANDALONE);
    miniSDC.startSDC();
    serverURI = miniSDC.getServerURI();
    miniSDC.createPipeline(getPipelineJson());
    ruleDefinitions = miniSDC.createRules(getPipelineName(), getPipelineRev(), getRuleDefinitions());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    KafkaTestUtil.shutdown();
    miniSDCTestingUtility.stopMiniSDC();
  }

  private static String getPipelineJson() throws Exception {
    URI uri = Resources.getResource("kafka_destination_pipeline_operations.json").toURI();
    String pipelineJson =  new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
    pipelineJson = pipelineJson.replace("topicName", TOPIC);
    pipelineJson = pipelineJson.replaceAll("localhost:9092", KafkaTestUtil.getMetadataBrokerURI());
    pipelineJson = pipelineJson.replaceAll("localhost:2181", KafkaTestUtil.getZkServer().connectString());
    return pipelineJson;
  }

  static String getPipelineName() {
    return "kafka_destination_pipeline";
  }

  static String getPipelineRev() {
    return "0";
  }

  private static String getRuleDefinitions() throws URISyntaxException, IOException {
    URI uri = Resources.getResource("ruleDefinitions.json").toURI();
    String pipelineJson =  new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
    return pipelineJson;
  }

  @Test(timeout = 20000)
  public void testRules() throws Exception {

    //Data rule Ids from the rule definition json
    String dataRuleId1 = "dataRule1437173920545";
    String dataRuleId2 = "dataRule1437173981080";
    String dataRuleId3 = "dataRule1437174013953";

    //Expected alert names for the above rule definitions
    String alertId1 = "alert.dataRule1437173920545.gauge";
    String alertId2 = "alert.dataRule1437173981080.gauge";
    String alertId3 = "alert.dataRule1437174013953.gauge";

    //Expected alert count for the rules
    int expectedCountAlert1 = 100;
    int expectedCountAlert2 = 50;
    int expectedCountAlert3 = 25;

    Map<String, Map<String, Object>> gauges = null;
    int alertsPresent = 0;

    //Wait until all 3 alerts are fired.
    while(alertsPresent != 3) {
      gauges = VerifyUtils.getGaugesFromMetrics(serverURI, getPipelineName(), getPipelineRev());
      Assert.assertNotNull(gauges);
      alertsPresent = 0;
      for (Map.Entry<String, Map<String, Object>> e : gauges.entrySet()) {
        if (e.getKey().contains("alert.")) {
          alertsPresent++;
        }
      }
    }

    //check each alert
    Assert.assertTrue(gauges.containsKey(alertId1));
    Map<String, Object> map = (Map<String, Object>) gauges.get(alertId1).get("value");
    Assert.assertTrue((Integer) map.get("currentValue") >= expectedCountAlert1);

    Assert.assertTrue(gauges.containsKey(alertId2));
    map = (Map<String, Object>) gauges.get(alertId2).get("value");
    Assert.assertTrue((Integer)map.get("currentValue") >= expectedCountAlert2);

    Assert.assertTrue(gauges.containsKey(alertId3));
    map = (Map<String, Object>) gauges.get(alertId3).get("value");
    Assert.assertTrue((Integer)map.get("currentValue") >= expectedCountAlert3);
    //change rule definitions to a large number before attempting to delete
    ruleDefinitions = ruleDefinitions.replaceAll("\"thresholdValue\" : \"100\"", "\"thresholdValue\" : \"10000000\"");
    ruleDefinitions = ruleDefinitions.replaceAll("\"thresholdValue\" : \"50\"", "\"thresholdValue\" : \"5000000\"");
    ruleDefinitions = ruleDefinitions.replaceAll("\"thresholdValue\" : \"25\"", "\"thresholdValue\" : \"2500000\"");
    ruleDefinitions = miniSDC.createRules(getPipelineName(), getPipelineRev(), ruleDefinitions);

    printGuages(gauges);
    //delete alert and wait until alerts are cleared.
    // It takes about 2 seconds for the rules config loader to load changes to rules.
    boolean alertRemoved = false;
    while(!alertRemoved) {
      VerifyUtils.deleteAlert(serverURI, getPipelineName(), getPipelineRev(), dataRuleId1);
      gauges = VerifyUtils.getGaugesFromMetrics(serverURI, getPipelineName(), getPipelineRev());
      Assert.assertNotNull(gauges);
      alertRemoved = !gauges.containsKey(alertId1);
    }
    printGuages(gauges);
    //changes have been reflected, delete without waiting
    Assert.assertTrue(gauges.containsKey(alertId2));
    Assert.assertTrue(gauges.containsKey(alertId3));


    VerifyUtils.deleteAlert(serverURI, getPipelineName(), getPipelineRev(), dataRuleId2);
    gauges = VerifyUtils.getGaugesFromMetrics(serverURI, getPipelineName(), getPipelineRev());
    printGuages(gauges);
    Assert.assertNotNull(gauges);
    Assert.assertFalse(gauges.containsKey(alertId1));
    Assert.assertFalse(gauges.containsKey(alertId2));
    Assert.assertTrue(gauges.containsKey(alertId3));


    VerifyUtils.deleteAlert(serverURI, getPipelineName(), getPipelineRev(), dataRuleId3);
    gauges = VerifyUtils.getGaugesFromMetrics(serverURI, getPipelineName(), getPipelineRev());
    printGuages(gauges);
    Assert.assertNotNull(gauges);
    Assert.assertFalse(gauges.containsKey(alertId1));
    Assert.assertFalse(gauges.containsKey(alertId2));
    Assert.assertFalse(gauges.containsKey(alertId3));

    //zero alerts should be present at this point present
    alertsPresent = 0;
    for (Map.Entry<String, Map<String, Object>> e : gauges.entrySet()) {
      if (e.getKey().contains("alert.")) {
        alertsPresent++;
      }
    }
    Assert.assertEquals(0, alertsPresent);

    //recreate new rule definitions
    ruleDefinitions = ruleDefinitions.replaceAll("\"thresholdValue\" : \"10000000\"", "\"thresholdValue\" : \"" +
      expectedCountAlert1 + "\"");
    ruleDefinitions = ruleDefinitions.replaceAll("\"thresholdValue\" : \"5000000\"", "\"thresholdValue\" : \"" +
      expectedCountAlert2 + "\"");
    ruleDefinitions = ruleDefinitions.replaceAll("\"thresholdValue\" : \"2500000\"", "\"thresholdValue\" : \"" +
      expectedCountAlert3 + "\"");
    ruleDefinitions = miniSDC.createRules(getPipelineName(), getPipelineRev(), ruleDefinitions);

    alertsPresent = 0;
    while(alertsPresent != 3) {
      gauges = VerifyUtils.getGaugesFromMetrics(serverURI, getPipelineName(), getPipelineRev());
      Assert.assertNotNull(gauges);

      //Expected 3 alerts
      alertsPresent = 0;
      for (Map.Entry<String, Map<String, Object>> e : gauges.entrySet()) {
        if (e.getKey().contains("alert.")) {
          alertsPresent++;
        }
      }
    }

    //check for alerts
    Assert.assertTrue(gauges.containsKey(alertId1));
    map = (Map<String, Object>) gauges.get(alertId1).get("value");
    Integer currentValue = (Integer) map.get("currentValue");
    Assert.assertTrue("Current value is : " + currentValue, currentValue >= expectedCountAlert1);

    Assert.assertTrue(gauges.containsKey(alertId2));
    map = (Map<String, Object>) gauges.get(alertId2).get("value");
    Assert.assertTrue((Integer)map.get("currentValue") >= expectedCountAlert2);

    Assert.assertTrue(gauges.containsKey(alertId3));
    map = (Map<String, Object>) gauges.get(alertId3).get("value");
    Assert.assertTrue((Integer)map.get("currentValue") >= expectedCountAlert3);
  }

  private static void printGuages(Map<String, Map<String, Object>> guages) {
    LOG.debug("Printing guages");
    for (Map.Entry<String, Map<String, Object>> mapEntry : guages.entrySet()) {
      LOG.debug("Entry key " + mapEntry.getKey());
      for (Map.Entry<String, Object> mapEntryKey : mapEntry.getValue().entrySet()) {
        LOG.debug("Value pair: " + mapEntryKey.getKey() + ": " + mapEntry.getValue());
      }
    }
  }
}
