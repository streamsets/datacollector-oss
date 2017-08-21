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
import com.streamsets.datacollector.config.DriftRuleDefinition;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.ThresholdType;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.runner.production.RulesConfigurationChangeRequest;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.util.TestUtil;
import dagger.ObjectGraph;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class TestRulesConfigLoader {

  /*must be static and initialized in before class other wise an attempt to recreate a pipeline will fail*/
  private static PipelineStoreTask pipelineStoreTask;
  private static Configuration configuration;
  private ProductionObserver observer;
  private BlockingQueue<Object> productionObserveRequests;

  @BeforeClass
  public static void beforeClass() throws IOException {
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, "target/var");
    File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    TestUtil.captureStagesForProductionRun();
    ObjectGraph g = ObjectGraph.create(TestUtil.TestPipelineStoreModuleNew.class);
    pipelineStoreTask = g.get(PipelineStoreTask.class);
    configuration = g.get(Configuration.class);
  }

  @AfterClass
  public static void afterClass() throws IOException {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
  }

  @Before
  public void setUp() {
    productionObserveRequests = new ArrayBlockingQueue<>(10, true /*FIFO*/);
    observer = new ProductionObserver(new Configuration(), null);
    observer.setObserveRequests(productionObserveRequests);
  }

  @Test
  public void testRulesConfigLoader() throws PipelineException, InterruptedException {
    RulesConfigLoader rulesConfigLoader = new RulesConfigLoader(
        TestUtil.MY_PIPELINE,
        TestUtil.PIPELINE_REV,
        pipelineStoreTask,
        configuration
    );
    RuleDefinitions ruleDefinitions = rulesConfigLoader.load(observer);
    observer.reconfigure();
    Assert.assertEquals(1, productionObserveRequests.size());
    Assert.assertNotNull(ruleDefinitions);
  }

  @Test
  public void testRulesConfigLoaderWithPreviousConfiguration() throws PipelineException, InterruptedException {
    RulesConfigLoader rulesConfigLoader = new RulesConfigLoader(
        TestUtil.MY_PIPELINE,
        TestUtil.PIPELINE_REV,
        pipelineStoreTask,
        configuration
    );
    long timestamp = System.currentTimeMillis();
    //create a DataRuleDefinition for one of the stages
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("myID", "myLabel", "p", 20, 10,
      "${record:value(\"/\")==4}", true, "alertText", ThresholdType.COUNT, "20", 100, true, false, true,
      timestamp);
    DataRuleDefinition dataRuleDefinition2 = new DataRuleDefinition("myID2", "myLabel", "p", 20, 10,
      "${record:value(\"/\")==4}", true, "alertText", ThresholdType.COUNT, "20", 100, true, false, true,
      timestamp);
    List<DataRuleDefinition> dataRuleDefinitions = new ArrayList<>();

    dataRuleDefinitions.add(dataRuleDefinition);
    dataRuleDefinitions.add(dataRuleDefinition2);

    DriftRuleDefinition driftRuleDefinition = new DriftRuleDefinition("myDriftID", "myDriftLabel", "p", 20, 10,
        "${record:value(\"/\")==4}", true, "alertText", true, false, true, timestamp);
    List<DriftRuleDefinition> driftRuleDefinitions = new ArrayList<>();

    driftRuleDefinitions.add(driftRuleDefinition);

    RuleDefinitions prevRuleDef = new RuleDefinitions(
        PipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
        RuleDefinitionsConfigBean.VERSION,
        Collections.<MetricsRuleDefinition>emptyList(),
        dataRuleDefinitions,
        driftRuleDefinitions,
        Collections.<String>emptyList(),
        UUID.randomUUID(),
        Collections.emptyList()
    );
    //The latest rule definition has just one data rule definition.
    //The old one had 2 data and 1 drift rule
    //Also there is a change in the condition of the data rule definition
    rulesConfigLoader.setPreviousRuleDefinitions(prevRuleDef);

    rulesConfigLoader.load(observer);
    //to get the RulesConfigurationChangeRequest into the queue we need to reconfigure observer
    observer.reconfigure();
    Assert.assertEquals(1, productionObserveRequests.size());
    RulesConfigurationChangeRequest rulesConfigurationChangeRequest = (RulesConfigurationChangeRequest) productionObserveRequests.take();
    Assert.assertNotNull(rulesConfigurationChangeRequest);
    Assert.assertEquals(3, rulesConfigurationChangeRequest.getRulesToRemove().size());
  }

}
