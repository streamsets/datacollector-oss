/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.restapi;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.restapi.bean.*;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.validation.RuleIssue;
import com.streamsets.pipeline.validation.ValidationError;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

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

  @BeforeClass
  public static void beforeClass() {

  }

  @Test
  public void testCreate() {
    boolean exceptionThrown = false;
    try {
      Response response = target("/v1/pipeline-library/myPipeline").queryParam("description", "my description").request()
        .put(Entity.json("abc"));
    } catch (Exception ex) {
      Assert.assertEquals(ex.getCause().getMessage(), "This operation is not supported in SLAVE mode");
      exceptionThrown = true;
    } finally {
      Assert.assertTrue(exceptionThrown);
    }
  }

  @Test
  public void testDelete() {
    boolean exceptionThrown = false;
    try {
      Response response = target("/v1/pipeline-library/myPipeline").request().delete();
    } catch (Exception ex) {
      Assert.assertEquals(ex.getCause().getMessage(), "This operation is not supported in SLAVE mode");
      exceptionThrown = true;
    } finally {
      Assert.assertTrue(exceptionThrown);
    }
  }

  @Test
  public void testSave() {
    com.streamsets.pipeline.config.PipelineConfiguration toSave = MockStages.createPipelineConfigurationSourceProcessorTarget();
    boolean exceptionThrown = false;
    try {
      Response response = target("/v1/pipeline-library/myPipeline")
        .queryParam("tag", "tag")
        .queryParam("tagDescription", "tagDescription").request()
        .post(Entity.json(BeanHelper.wrapPipelineConfiguration(toSave)));
    } catch (Exception ex) {
      Assert.assertEquals(ex.getCause().getMessage(), "This operation is not supported in SLAVE mode");
      exceptionThrown = true;
    } finally {
      Assert.assertTrue(exceptionThrown);
    }
  }

  @Override
  protected Application configure() {
    return new ResourceConfig() {
      {
        register(new PipelineStoreResourceConfig());
        register(PipelineStoreResource.class);
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

      com.streamsets.pipeline.util.TestUtil.captureStagesForProductionRun();

      PipelineStoreTask pipelineStore = Mockito.mock(PipelineStoreTask.class);
      try {
        Mockito.when(pipelineStore.getPipelines()).thenReturn(ImmutableList.of(
            new com.streamsets.pipeline.store.PipelineInfo("name", "description", new java.util.Date(0), new java.util.Date(0), "creator",
                "lastModifier", "1", UUID.randomUUID(), true)));
        Mockito.when(pipelineStore.getInfo("xyz")).thenReturn(
            new com.streamsets.pipeline.store.PipelineInfo("xyz", "xyz description",new java.util.Date(0), new java.util.Date(0), "xyz creator",
                "xyz lastModifier", "1", UUID.randomUUID(), true));
        Mockito.when(pipelineStore.getHistory("xyz")).thenReturn(ImmutableList.of(
          new com.streamsets.pipeline.store.PipelineRevInfo(new com.streamsets.pipeline.store.PipelineInfo("xyz",
            "xyz description", new java.util.Date(0), new java.util.Date(0), "xyz creator",
                "xyz lastModifier", "1", UUID.randomUUID(), true))));
        Mockito.when(pipelineStore.load("xyz", "1.0.0")).thenReturn(
            MockStages.createPipelineConfigurationSourceProcessorTarget());
        Mockito.when(pipelineStore.create("nobody", "myPipeline", "my description")).thenReturn(
            MockStages.createPipelineConfigurationSourceProcessorTarget());
        Mockito.doNothing().when(pipelineStore).delete("myPipeline");
        Mockito.doThrow(new PipelineStoreException(ContainerError.CONTAINER_0200, "xyz"))
            .when(pipelineStore).delete("xyz");
        Mockito.when(pipelineStore.save(
            Matchers.anyString(), Matchers.anyString(), Matchers.anyString(), Matchers.anyString(),
            (com.streamsets.pipeline.config.PipelineConfiguration)Matchers.any())).thenReturn(
          MockStages.createPipelineConfigurationSourceProcessorTarget());

        List<MetricsRuleDefinitionJson> metricsRuleDefinitionJsons = new ArrayList<>();
        metricsRuleDefinitionJsons.add(new MetricsRuleDefinitionJson("m1", "m1", "a", MetricTypeJson.COUNTER,
          MetricElementJson.COUNTER_COUNT, "p", false, true));
        metricsRuleDefinitionJsons.add(new MetricsRuleDefinitionJson("m2", "m2", "a", MetricTypeJson.TIMER,
          MetricElementJson.TIMER_M15_RATE, "p", false, true));
        metricsRuleDefinitionJsons.add(new MetricsRuleDefinitionJson("m3", "m3", "a", MetricTypeJson.HISTOGRAM,
          MetricElementJson.HISTOGRAM_MEAN, "p", false, true));

        List<DataRuleDefinitionJson> dataRuleDefinitionJsons = new ArrayList<>();
        dataRuleDefinitionJsons.add(new DataRuleDefinitionJson("a", "a", "a", 20, 300, "x", true, "c", ThresholdTypeJson.COUNT, "200",
          1000, true, false,true));
        dataRuleDefinitionJsons.add(new DataRuleDefinitionJson("b", "b", "b", 20, 300, "x", true, "c", ThresholdTypeJson.COUNT, "200",
          1000, true, false, true));
        dataRuleDefinitionJsons.add(new DataRuleDefinitionJson("c", "c", "c", 20, 300, "x", true, "c", ThresholdTypeJson.COUNT, "200",
          1000, true, false, true));

        RuleDefinitionsJson rules = new RuleDefinitionsJson(metricsRuleDefinitionJsons, dataRuleDefinitionJsons,
          Collections.<String>emptyList(), UUID.randomUUID());
        List<RuleIssue> ruleIssues = new ArrayList<>();
        ruleIssues.add(RuleIssue.createRuleIssue("a", ValidationError.VALIDATION_0000));
        ruleIssues.add(RuleIssue.createRuleIssue("b", ValidationError.VALIDATION_0000));
        ruleIssues.add(RuleIssue.createRuleIssue("c", ValidationError.VALIDATION_0000));
        rules.getRuleDefinitions().setRuleIssues(ruleIssues);

        try {
          Mockito.when(pipelineStore.retrieveRules("myPipeline", "tag")).thenReturn(rules.getRuleDefinitions());
        } catch (PipelineStoreException e) {
          e.printStackTrace();
        }
        try {
          Mockito.when(pipelineStore.storeRules(
            Matchers.anyString(), Matchers.anyString(), (com.streamsets.pipeline.config.RuleDefinitions) Matchers.any()))
            .thenReturn(rules.getRuleDefinitions());
        } catch (PipelineStoreException e) {
          e.printStackTrace();
        }

      } catch (PipelineStoreException e) {
        e.printStackTrace();
      }
      return pipelineStore;
    }

    @Override
    public void dispose(PipelineStoreTask pipelineStore) {
    }
  }

}
