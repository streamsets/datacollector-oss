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
package com.streamsets.datacollector.restapi;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.ConfigConfigurationJson;
import com.streamsets.datacollector.restapi.bean.DataRuleDefinitionJson;
import com.streamsets.datacollector.restapi.bean.DriftRuleDefinitionJson;
import com.streamsets.datacollector.restapi.bean.MetricElementJson;
import com.streamsets.datacollector.restapi.bean.MetricTypeJson;
import com.streamsets.datacollector.restapi.bean.MetricsRuleDefinitionJson;
import com.streamsets.datacollector.restapi.bean.RuleDefinitionsJson;
import com.streamsets.datacollector.restapi.bean.ThresholdTypeJson;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.store.impl.SlavePipelineStoreTask;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.validation.RuleIssue;
import com.streamsets.datacollector.validation.ValidationError;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class TestPipelineStoreResourceForSlaveMode extends JerseyTest {

  private static final Logger LOG = LoggerFactory.getLogger(TestPipelineStoreResourceForSlaveMode.class);

  @BeforeClass
  public static void beforeClass() {

  }

  @Test
  public void testCreate() {
    try {
      Response response = target("/v1/pipeline/myPipeline").queryParam("description", "my description").request()
        .put(Entity.json("abc"));
      Assert.fail("Expected exception but didn't get any");
    } catch (Exception ex) {
      // Expected
    }
  }

  @Test
  public void testDelete() {
    try {
      Response response = target("/v1/pipeline/myPipeline").request().delete();
      Assert.fail("Expected exception but didn't get any");
    } catch (Exception ex) {
      // expected
    }
  }

  @Test
  public void testSave() {
    com.streamsets.datacollector.config.PipelineConfiguration toSave = MockStages.createPipelineConfigurationSourceProcessorTarget();
    boolean exceptionThrown = false;
    try {
      Response response = target("/v1/pipeline/myPipeline")
        .queryParam("tag", "tag")
        .queryParam("tagDescription", "tagDescription").request()
        .post(Entity.json(BeanHelper.wrapPipelineConfiguration(toSave)));
      Assert.fail("Expected exception but didn't get any");
    } catch (Exception ex) {
      // expected
    } finally {
    }
  }

  @Override
  protected Application configure() {
    return new ResourceConfig() {
      {
        register(new PipelineStoreResourceConfig());
        register(PipelineStoreResource.class);
        register(MultiPartFeature.class);
      }
    };
  }

  static class PipelineStoreResourceConfig extends AbstractBinder {
    @Override
    protected void configure() {
      bindFactory(PipelineStoreTestInjector.class).to(PipelineStoreTask.class);
      bindFactory(TestUtil.StageLibraryTestInjector.class).to(StageLibraryTask.class);
      bindFactory(TestUtil.PrincipalTestInjector.class).to(Principal.class);
      bindFactory(TestUtil.URITestInjector.class).to(URI.class);
      bindFactory(TestUtil.RuntimeInfoTestInjectorForSlaveMode.class).to(RuntimeInfo.class);
    }
  }

  static class PipelineStoreTestInjector implements Factory<PipelineStoreTask> {

    public PipelineStoreTestInjector() {
    }

    @Singleton
    @Override
    public PipelineStoreTask provide() {

      com.streamsets.datacollector.util.TestUtil.captureStagesForProductionRun();

      PipelineStoreTask pipelineStore = Mockito.mock(PipelineStoreTask.class);
      try {
        Mockito.when(pipelineStore.getPipelines()).thenReturn(ImmutableList.of(
            new com.streamsets.datacollector.store.PipelineInfo("name", "label", "description", new java.util.Date(0), new java.util.Date(0), "creator",
                "lastModifier", "1", UUID.randomUUID(), true, null, "x", "y")));
        Mockito.when(pipelineStore.getInfo("xyz")).thenReturn(
            new com.streamsets.datacollector.store.PipelineInfo("xyz", "xyz label", "xyz description",new java.util.Date(0), new java.util.Date(0), "xyz creator",
                "xyz lastModifier", "1", UUID.randomUUID(), true, null, "x", "y"));
        Mockito.when(pipelineStore.getHistory("xyz")).thenReturn(ImmutableList.of(
          new com.streamsets.datacollector.store.PipelineRevInfo(new com.streamsets.datacollector.store.PipelineInfo("xyz",
              "xyz label","xyz description", new java.util.Date(0), new java.util.Date(0), "xyz creator",
                "xyz lastModifier", "1", UUID.randomUUID(), true, null, "x", "y"))));
        Mockito.when(pipelineStore.load("xyz", "1.0.0")).thenReturn(
            MockStages.createPipelineConfigurationSourceProcessorTarget());
        Mockito.when(pipelineStore.create("nobody", "myPipeline", "myPipeline", "my description", false, false)).thenReturn(
            MockStages.createPipelineConfigurationSourceProcessorTarget());
        Mockito.doNothing().when(pipelineStore).delete("myPipeline");
        Mockito.doThrow(new PipelineStoreException(ContainerError.CONTAINER_0200, "xyz"))
            .when(pipelineStore).delete("xyz");
        Mockito.when(pipelineStore.save(
            Matchers.anyString(), Matchers.anyString(), Matchers.anyString(), Matchers.anyString(),
            (com.streamsets.datacollector.config.PipelineConfiguration)Matchers.any())).thenReturn(
          MockStages.createPipelineConfigurationSourceProcessorTarget());

        List<MetricsRuleDefinitionJson> metricsRuleDefinitionJsons = new ArrayList<>();
        long timestamp = System.currentTimeMillis();
        metricsRuleDefinitionJsons.add(new MetricsRuleDefinitionJson("m1", "m1", "a", MetricTypeJson.COUNTER,
          MetricElementJson.COUNTER_COUNT, "p", false, true, timestamp));
        metricsRuleDefinitionJsons.add(new MetricsRuleDefinitionJson("m2", "m2", "a", MetricTypeJson.TIMER,
          MetricElementJson.TIMER_M15_RATE, "p", false, true, timestamp));
        metricsRuleDefinitionJsons.add(new MetricsRuleDefinitionJson("m3", "m3", "a", MetricTypeJson.HISTOGRAM,
          MetricElementJson.HISTOGRAM_MEAN, "p", false, true, timestamp));

        List<DataRuleDefinitionJson> dataRuleDefinitionJsons = new ArrayList<>();
        dataRuleDefinitionJsons.add(new DataRuleDefinitionJson("a", "a", "a", 20, 300, "x", true, "c", ThresholdTypeJson.COUNT, "200",
          1000, true, false,true, timestamp));
        dataRuleDefinitionJsons.add(new DataRuleDefinitionJson("b", "b", "b", 20, 300, "x", true, "c", ThresholdTypeJson.COUNT, "200",
          1000, true, false, true, timestamp));
        dataRuleDefinitionJsons.add(new DataRuleDefinitionJson("c", "c", "c", 20, 300, "x", true, "c", ThresholdTypeJson.COUNT, "200",
          1000, true, false, true, timestamp));

        RuleDefinitionsJson rules = new RuleDefinitionsJson(
            PipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
            RuleDefinitionsConfigBean.VERSION,
            metricsRuleDefinitionJsons,
            dataRuleDefinitionJsons,
            Collections.<DriftRuleDefinitionJson>emptyList(),
            Collections.<String>emptyList(),
            UUID.randomUUID(),
            Collections.<ConfigConfigurationJson>emptyList()
        );
        List<RuleIssue> ruleIssues = new ArrayList<>();
        ruleIssues.add(RuleIssue.createRuleIssue("a", ValidationError.VALIDATION_0000));
        ruleIssues.add(RuleIssue.createRuleIssue("b", ValidationError.VALIDATION_0000));
        ruleIssues.add(RuleIssue.createRuleIssue("c", ValidationError.VALIDATION_0000));
        rules.getRuleDefinitions().setRuleIssues(ruleIssues);

        try {
          Mockito.when(pipelineStore.retrieveRules("myPipeline", "tag")).thenReturn(rules.getRuleDefinitions());
        } catch (PipelineStoreException e) {
          LOG.debug("Ignoring exception", e);
        }
        try {
          Mockito.when(pipelineStore.storeRules(
              Matchers.anyString(),
              Matchers.anyString(),
              (com.streamsets.datacollector.config.RuleDefinitions) Matchers.any(),
              Mockito.anyBoolean()
          )).thenReturn(rules.getRuleDefinitions());
        } catch (PipelineStoreException e) {
          LOG.debug("Ignoring exception", e);
        }

      } catch (com.streamsets.datacollector.util.PipelineException e) {
        LOG.debug("Ignoring exception", e);
      }
      return new SlavePipelineStoreTask(pipelineStore);
    }

    @Override
    public void dispose(PipelineStoreTask pipelineStore) {
    }
  }

}
