/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.alerts;

import com.google.common.collect.EvictingQueue;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.SamplingDefinition;
import com.streamsets.pipeline.el.ELBasicSupport;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.el.ELStringSupport;
import com.streamsets.pipeline.observerstore.ObserverStore;
import com.streamsets.pipeline.observerstore.impl.FileObserverStore;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class TestRecordSampler {

  private static ELEvaluator elEvaluator;
  private static ELEvaluator.Variables variables;
  private static ObserverStore observerStore;

  @BeforeClass
  public static void setUp() {
    variables = new ELEvaluator.Variables();
    elEvaluator = new ELEvaluator();
    ELBasicSupport.registerBasicFunctions(elEvaluator);
    ELRecordSupport.registerRecordFunctions(elEvaluator);
    ELStringSupport.registerStringFunctions(elEvaluator);
    observerStore = Mockito.mock(FileObserverStore.class);
    Mockito.doNothing().when(observerStore).storeSampledRecords(
      Matchers.anyString(), Matchers.anyString(), Matchers.anyMap());
  }

  @Test
  public void testRecordSampler() {
    SamplingDefinition samplingDefinition = new SamplingDefinition("testRecordSampler", "testRecordSampler", "testRecordSampler",
      "${record:value(\"/name\")!=null}", "75", true);
    RecordSampler recordSampler = new RecordSampler("p", "1", samplingDefinition, observerStore, variables, elEvaluator);
    Map<String, EvictingQueue<Record>> result = new HashMap<>();
    recordSampler.sample(TestUtil.createSnapshot("testRecordSampler"), result);
    Assert.assertTrue(result.size() >= 1);
  }

  @Test
  public void testRecordSamplerDisabled() {
    SamplingDefinition samplingDefinition = new SamplingDefinition("testRecordSampler", "testRecordSampler", "testRecordSampler",
      "${record:value(\"/name\")!=null}", "75", false);
    RecordSampler recordSampler = new RecordSampler("p", "1", samplingDefinition, observerStore, variables, elEvaluator);
    Map<String, EvictingQueue<Record>> result = new HashMap<>();
    recordSampler.sample(TestUtil.createSnapshot("testRecordSampler"), result);
    Assert.assertTrue(result.size() == 0);
  }
}
