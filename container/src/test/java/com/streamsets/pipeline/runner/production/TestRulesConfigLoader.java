/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.config.DataRuleDefinition;
import com.streamsets.pipeline.config.MetricsAlertDefinition;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.config.ThresholdType;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.util.TestUtil;
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
  private ProductionObserver observer;
  private BlockingQueue<Object> productionObserveRequests;

  @BeforeClass
  public static void beforeClass() throws IOException {
    System.setProperty(RuntimeInfo.DATA_DIR, "target/var");
    File f = new File(System.getProperty(RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    TestUtil.captureStagesForProductionRun();
    ObjectGraph g = ObjectGraph.create(TestUtil.TestPipelineStoreModule.class);
    pipelineStoreTask = g.get(PipelineStoreTask.class);
  }

  @AfterClass
  public static void afterClass() throws IOException {
    System.getProperties().remove(RuntimeInfo.DATA_DIR);
  }

  @Before
  public void setUp() {
    productionObserveRequests = new ArrayBlockingQueue<>(10, true /*FIFO*/);
    observer = new ProductionObserver(productionObserveRequests);
  }

  @Test
  public void testRulesConfigLoader() throws PipelineStoreException, InterruptedException {
    RulesConfigLoader rulesConfigLoader = new RulesConfigLoader(TestUtil.MY_PIPELINE, TestUtil.PIPELINE_REV,
      pipelineStoreTask);
    RuleDefinition ruleDefinition = rulesConfigLoader.load(observer);
    observer.reconfigure();
    Assert.assertEquals(1, productionObserveRequests.size());
    Assert.assertNotNull(ruleDefinition);
  }

  @Test
  public void testRulesConfigLoaderWithPreviousConfiguration() throws PipelineStoreException, InterruptedException {
    RulesConfigLoader rulesConfigLoader = new RulesConfigLoader(TestUtil.MY_PIPELINE, TestUtil.PIPELINE_REV,
      pipelineStoreTask);

    //create a DataRuleDefinition for one of the stages
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("myID", "myLabel", "p", 20, 10,
      "${record:value(\"/\")==4}", true, "alertText", ThresholdType.COUNT, "20", 100, true, false, true);
    DataRuleDefinition dataRuleDefinition2 = new DataRuleDefinition("myID2", "myLabel", "p", 20, 10,
      "${record:value(\"/\")==4}", true, "alertText", ThresholdType.COUNT, "20", 100, true, false, true);
    List<DataRuleDefinition> dataRuleDefinitions = new ArrayList<>();

    dataRuleDefinitions.add(dataRuleDefinition);
    dataRuleDefinitions.add(dataRuleDefinition2);
    RuleDefinition prevRuleDef = new RuleDefinition(Collections.<MetricsAlertDefinition>emptyList(),
      dataRuleDefinitions, Collections.<String>emptyList(), UUID.randomUUID());
    //The latest rule definition has just one data rule definition.
    //The old one had 2
    //Also there is a change in the condition of the data rule definition
    rulesConfigLoader.setPreviousRuleDefinition(prevRuleDef);

    rulesConfigLoader.load(observer);
    //to get the RulesConfigurationChangeRequest into the queue we need to reconfigure observer
    observer.reconfigure();
    Assert.assertEquals(1, productionObserveRequests.size());
    RulesConfigurationChangeRequest rulesConfigurationChangeRequest = (RulesConfigurationChangeRequest) productionObserveRequests.take();
    Assert.assertNotNull(rulesConfigurationChangeRequest);
    Assert.assertEquals(2, rulesConfigurationChangeRequest.getRulesToRemove().size());
  }

}
