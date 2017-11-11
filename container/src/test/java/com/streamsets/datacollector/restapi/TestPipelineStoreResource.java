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
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.manager.PipelineStateImpl;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.ConfigConfigurationJson;
import com.streamsets.datacollector.restapi.bean.DataRuleDefinitionJson;
import com.streamsets.datacollector.restapi.bean.DriftRuleDefinitionJson;
import com.streamsets.datacollector.restapi.bean.MetricElementJson;
import com.streamsets.datacollector.restapi.bean.MetricTypeJson;
import com.streamsets.datacollector.restapi.bean.MetricsRuleDefinitionJson;
import com.streamsets.datacollector.restapi.bean.MultiStatusResponseJson;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.PipelineEnvelopeJson;
import com.streamsets.datacollector.restapi.bean.PipelineInfoJson;
import com.streamsets.datacollector.restapi.bean.PipelineRevInfoJson;
import com.streamsets.datacollector.restapi.bean.PipelineStateJson;
import com.streamsets.datacollector.restapi.bean.RuleDefinitionsJson;
import com.streamsets.datacollector.restapi.bean.StageConfigurationJson;
import com.streamsets.datacollector.restapi.bean.ThresholdTypeJson;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.store.impl.FileAclStoreTask;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.validation.RuleIssue;
import com.streamsets.datacollector.validation.ValidationError;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.lib.security.acl.dto.Action;
import com.streamsets.lib.security.acl.dto.Permission;
import com.streamsets.lib.security.acl.dto.ResourceType;
import com.streamsets.lib.security.acl.dto.SubjectType;
import com.streamsets.pipeline.api.ExecutionMode;
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
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestPipelineStoreResource extends JerseyTest {

  private static Logger LOG = LoggerFactory.getLogger(TestPipelineStoreResource.class);

  @BeforeClass
  public static void beforeClass() {
  }

  @Test
  public void testGetPipelinesCount() {
    // when ACL is disabled
    isACLEnabled = false;
    Response response = target("/v1/pipelines/count").request().get();
    Map<String, Object> countRes = (Map<String, Object>)response.readEntity(Map.class);
    Assert.assertNotNull(countRes);
    Assert.assertEquals(7, countRes.get("count"));

    // when ACL is enabled
    isACLEnabled = true;
    response = target("/v1/pipelines/count").request().get();
    countRes = (Map<String, Object>)response.readEntity(Map.class);
    Assert.assertNotNull(countRes);
    Assert.assertEquals(6, countRes.get("count"));
  }

  @Test
  public void testGetSystemPipelineLabels() {
    Response response = target("/v1/pipelines/systemLabels").request().get();
    List systemPipelineLabels = response.readEntity(List.class);
    Assert.assertNotNull(systemPipelineLabels);
    Assert.assertTrue(systemPipelineLabels.size() > 1);
  }

  @Test
  public void testGetPipelineLabels() {
    Response response = target("/v1/pipelines/labels").request().get();
    List systemPipelineLabels = response.readEntity(List.class);
    Assert.assertNotNull(systemPipelineLabels);
    Assert.assertTrue(systemPipelineLabels.size() == 1);
    Assert.assertEquals("label1", systemPipelineLabels.get(0));
  }

  @Test
  public void testGetPipelines() {
    // when ACL is disabled
    isACLEnabled = false;
    Response response = target("/v1/pipelines").request().get();
    List<PipelineInfoJson> pipelineInfoJsons = response.readEntity(new GenericType<List<PipelineInfoJson>>() {});
    Assert.assertNotNull(pipelineInfoJsons);
    Assert.assertEquals(7, pipelineInfoJsons.size());

    // when ACL is enabled
    isACLEnabled = true;
    response = target("/v1/pipelines").request().get();
    pipelineInfoJsons = response.readEntity(new GenericType<List<PipelineInfoJson>>() {});
    Assert.assertNotNull(pipelineInfoJsons);
    Assert.assertEquals(6, pipelineInfoJsons.size());
  }

  @Test
  public void testGetPipelinesFilteringByText() {
    Response response = target("/v1/pipelines")
        .queryParam("filterText", "name1")
        .request()
        .get();
    List<PipelineInfoJson> pipelineInfoJsons = response.readEntity(new GenericType<List<PipelineInfoJson>>() {});
    Assert.assertNotNull(pipelineInfoJsons);
    Assert.assertEquals(1, pipelineInfoJsons.size());
  }

  @Test
  public void testGetPipelinesFilteringByLabel() {
    Response response = target("/v1/pipelines")
        .queryParam("label", "label1")
        .request()
        .get();
    List<PipelineInfoJson> pipelineInfoJsons = response.readEntity(new GenericType<List<PipelineInfoJson>>() {});
    Assert.assertNotNull(pipelineInfoJsons);
    Assert.assertEquals(2, pipelineInfoJsons.size());
  }

  @Test
  public void testGetPipelinesSorting() {
    Response response = target("/v1/pipelines")
        .queryParam("orderBy", "NAME")
        .queryParam("order", "ASC")
        .request()
        .get();
    List<PipelineInfoJson> pipelineInfoJsons = response.readEntity(new GenericType<List<PipelineInfoJson>>() {});
    Assert.assertNotNull(pipelineInfoJsons);
    Assert.assertEquals(6, pipelineInfoJsons.size());
    Assert.assertEquals("name1", pipelineInfoJsons.get(0).getPipelineId());
    Assert.assertEquals("name2", pipelineInfoJsons.get(1).getPipelineId());
    Assert.assertEquals("name3", pipelineInfoJsons.get(2).getPipelineId());

    response = target("/v1/pipelines")
        .queryParam("orderBy", "NAME")
        .queryParam("order", "DESC")
        .request()
        .get();
    pipelineInfoJsons = response.readEntity(new GenericType<List<PipelineInfoJson>>() {});
    Assert.assertNotNull(pipelineInfoJsons);
    Assert.assertEquals(6, pipelineInfoJsons.size());
    Assert.assertEquals("readWriteOnly", pipelineInfoJsons.get(0).getPipelineId());
    Assert.assertEquals("readWriteExecute", pipelineInfoJsons.get(1).getPipelineId());
    Assert.assertEquals("readOnly", pipelineInfoJsons.get(2).getPipelineId());

    response = target("/v1/pipelines")
        .queryParam("orderBy", "STATUS")
        .queryParam("order", "ASC")
        .request()
        .get();
    pipelineInfoJsons = response.readEntity(new GenericType<List<PipelineInfoJson>>() {});
    Assert.assertNotNull(pipelineInfoJsons);
    Assert.assertEquals(6, pipelineInfoJsons.size());
    Assert.assertEquals("name2", pipelineInfoJsons.get(0).getPipelineId());
    Assert.assertEquals("name1", pipelineInfoJsons.get(1).getPipelineId());
    Assert.assertEquals("name3", pipelineInfoJsons.get(2).getPipelineId());
  }

  @Test
  public void testGetPipelinesPagination() {
    Response response = target("/v1/pipelines")
        .queryParam("offset", "1")
        .queryParam("len", "2")
        .queryParam("orderBy", "NAME")
        .queryParam("order", "ASC")
        .request()
        .get();
    List<PipelineInfoJson> pipelineInfoJsons = response.readEntity(new GenericType<List<PipelineInfoJson>>() {});
    Assert.assertNotNull(pipelineInfoJsons);
    Assert.assertEquals(2, pipelineInfoJsons.size());
    Assert.assertEquals("name2", pipelineInfoJsons.get(0).getPipelineId());
    Assert.assertEquals("name3", pipelineInfoJsons.get(1).getPipelineId());
    Assert.assertNotNull(response.getHeaders().get("TOTAL_COUNT"));
    Assert.assertEquals("6", response.getHeaders().get("TOTAL_COUNT").get(0));
  }


  @Test
  public void testGetPipelinesWithStatus() {
    Response response = target("/v1/pipelines")
        .queryParam("includeStatus", true)
        .request()
        .get();
    List responseObj = response.readEntity(new GenericType<List>() {});
    Assert.assertNotNull(responseObj);
    Assert.assertEquals(2, responseObj.size());
    Assert.assertNotNull(responseObj.get(0));

    List<PipelineInfoJson> pipelineInfoJsons = (List<PipelineInfoJson>) responseObj.get(0);
    Assert.assertNotNull(pipelineInfoJsons);
    Assert.assertEquals(6, pipelineInfoJsons.size());

    Assert.assertNotNull(responseObj.get(1));
    List<PipelineStateJson> pipelineStateJsons = (List<PipelineStateJson>) responseObj.get(1);
    Assert.assertNotNull(pipelineStateJsons);
    Assert.assertEquals(3, pipelineStateJsons.size());
  }

  @Test
  public void testGetInfoPipeline() {
    Response response = target("/v1/pipeline/xyz").queryParam("rev", "1").queryParam("get", "pipeline").
        request().get();
    PipelineConfigurationJson pipelineConfig = response.readEntity(PipelineConfigurationJson.class);
    Assert.assertNotNull(pipelineConfig);

    // test get Info on pipeline for which user doesn't have read permission
    boolean exceptionTriggered = false;
    try {
      response = target("/v1/pipeline/noPerm")
          .queryParam("get", "pipeline")
          .request()
          .get();
    } catch (Exception ex) {
      exceptionTriggered = true;
      Assert.assertTrue(ex.getCause().getMessage().contains("CONTAINER_01200"));
    }
    Assert.assertTrue(exceptionTriggered);

  }

  @Test
  public void testGetInfoInfo() {
    Response response = target("/v1/pipeline/xyz").queryParam("rev", "1.0.0").queryParam("get", "info").
        request().get();
    PipelineInfoJson pipelineInfoJson = response.readEntity(PipelineInfoJson.class);
    Assert.assertNotNull(pipelineInfoJson);
  }

  @Test
  public void testGetInfoHistory() {
    Response response = target("/v1/pipeline/xyz").queryParam("rev", "1.0.0").queryParam("get", "history").
        request().get();
    List<PipelineRevInfoJson> pipelineRevInfoJson = response.readEntity(new GenericType<List<PipelineRevInfoJson>>() {});
    Assert.assertNotNull(pipelineRevInfoJson);

    // test get Info on pipeline for which user doesn't have read permission
    boolean exceptionTriggered = false;
    try {
      response = target("/v1/pipeline/noPerm")
          .queryParam("get", "history")
          .request()
          .get();
    } catch (Exception ex) {
      exceptionTriggered = true;
      Assert.assertTrue(ex.getCause().getMessage().contains("CONTAINER_01200"));
    }
    Assert.assertTrue(exceptionTriggered);
  }

  @Test
  public void testCreate() {
    Response response = target("/v1/pipeline/myPipeline").queryParam("description", "my description").request()
        .put(Entity.json("abc"));
    PipelineConfigurationJson pipelineConfig = response.readEntity(PipelineConfigurationJson.class);
    Assert.assertEquals(201, response.getStatusInfo().getStatusCode());
    Assert.assertNotNull(pipelineConfig);
    Assert.assertNotNull(pipelineConfig.getUuid());
    Assert.assertEquals(3, pipelineConfig.getStages().size());

    StageConfigurationJson stageConf = pipelineConfig.getStages().get(0);
    Assert.assertEquals("s", stageConf.getInstanceName());
    Assert.assertEquals("sourceName", stageConf.getStageName());
    Assert.assertEquals("1", stageConf.getStageVersion());
    Assert.assertEquals("default", stageConf.getLibrary());

  }

  @Test
  public void testDelete() {
    Response response = target("/v1/pipeline/myPipeline").request().delete();
    Assert.assertEquals(200, response.getStatus());

    // test delete on pipeline for which user doesn't have write permission
    boolean exceptionTriggered = false;
    try {
      response = target("/v1/pipeline/noPerm")
          .request()
          .delete();
    } catch (Exception ex) {
      exceptionTriggered = true;
      Assert.assertTrue(ex.getCause().getMessage().contains("CONTAINER_01200"));
    }
    Assert.assertTrue(exceptionTriggered);

    exceptionTriggered = false;
    try {
      response = target("/v1/pipeline/readOnly")
          .request()
          .delete();
    } catch (Exception ex) {
      exceptionTriggered = true;
      Assert.assertTrue(ex.getCause().getMessage().contains("CONTAINER_01200"));
    }
    Assert.assertTrue(exceptionTriggered);
  }

  @Test
  public void testDeleteMultiplePipelines() {
    Response response = target("/v1/pipelines/delete").request()
        .post(Entity.json("[\"myPipeline1\", \"myPipeline2\"]"));
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testSave() {
    PipelineConfiguration toSave = MockStages.createPipelineConfigurationSourceProcessorTarget();
    Response response = target("/v1/pipeline/myPipeline")
        .queryParam("tag", "tag")
        .queryParam("tagDescription", "tagDescription").request()
        .post(Entity.json(BeanHelper.wrapPipelineConfiguration(toSave)));

    PipelineConfigurationJson returned = response.readEntity(PipelineConfigurationJson.class);
    Assert.assertNotNull(returned);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(toSave.getDescription(), returned.getDescription());
    //Assert.assertEquals("A", returned.getMetadata().get("a"));


    // test save on pipeline for which user doesn't have write permission
    boolean exceptionTriggered = false;
    try {
      response = target("/v1/pipeline/readOnly")
          .queryParam("tag", "tag")
          .queryParam("tagDescription", "tagDescription").request()
          .post(Entity.json(BeanHelper.wrapPipelineConfiguration(toSave)));
    } catch (Exception ex) {
      exceptionTriggered = true;
      Assert.assertTrue(ex.getCause().getMessage().contains("CONTAINER_01200"));
    }
    Assert.assertTrue(exceptionTriggered);

    // test save on pipeline created by other user and with write permission
    response = target("/v1/pipeline/readWriteOnly")
        .queryParam("tag", "tag")
        .queryParam("tagDescription", "tagDescription").request()
        .post(Entity.json(BeanHelper.wrapPipelineConfiguration(toSave)));
    returned = response.readEntity(PipelineConfigurationJson.class);
    Assert.assertNotNull(returned);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(toSave.getDescription(), returned.getDescription());
  }

  @Test
  public void testSaveRules() {

    long timestamp = System.currentTimeMillis();
    List<MetricsRuleDefinitionJson> metricsRuleDefinitionJsons = new ArrayList<>();
    metricsRuleDefinitionJsons.add(new MetricsRuleDefinitionJson("m1", "m1", "a", MetricTypeJson.COUNTER,
      MetricElementJson.COUNTER_COUNT, "p", false, true, timestamp));
    metricsRuleDefinitionJsons.add(new MetricsRuleDefinitionJson("m2", "m2", "a", MetricTypeJson.TIMER,
      MetricElementJson.TIMER_M15_RATE, "p", false, true, timestamp));
    metricsRuleDefinitionJsons.add(new MetricsRuleDefinitionJson("m3", "m3", "a", MetricTypeJson.HISTOGRAM,
      MetricElementJson.HISTOGRAM_MEAN, "p", false, true, timestamp));

    List<DataRuleDefinitionJson> dataRuleDefinitionJsons = new ArrayList<>();
    dataRuleDefinitionJsons.add(new DataRuleDefinitionJson("a", "a", "a", 20, 300, "x", true, "a", ThresholdTypeJson.COUNT, "200",
      1000, true, false, true, timestamp));
    dataRuleDefinitionJsons.add(new DataRuleDefinitionJson("b", "b", "b", 20, 300, "x", true, "a", ThresholdTypeJson.COUNT, "200",
      1000, true, false, true, timestamp));
    dataRuleDefinitionJsons.add(new DataRuleDefinitionJson("c", "c", "c", 20, 300, "x", true, "a", ThresholdTypeJson.COUNT, "200",
      1000, true, false, true, timestamp));

    RuleDefinitionsJson ruleDefinitionsJson = new RuleDefinitionsJson(
        PipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
        RuleDefinitionsConfigBean.VERSION,
        metricsRuleDefinitionJsons,
        dataRuleDefinitionJsons,
        Collections.<DriftRuleDefinitionJson>emptyList(),
        Collections.<String>emptyList(),
        UUID.randomUUID(),
        Collections.EMPTY_LIST
    );
    Response r = target("/v1/pipeline/myPipeline/rules").queryParam("rev", "tag").request()
      .post(Entity.json(ruleDefinitionsJson));
    RuleDefinitionsJson result = r.readEntity(RuleDefinitionsJson.class);

    Assert.assertEquals(3, result.getMetricsRuleDefinitions().size());
    Assert.assertEquals(3, result.getDataRuleDefinitions().size());

    // test save on pipeline rules for which user doesn't have write permission
    boolean exceptionTriggered = false;
    try {
      r = target("/v1/pipeline/readOnly/rules")
          .queryParam("rev", "tag").request()
          .post(Entity.json(ruleDefinitionsJson));
    } catch (Exception ex) {
      exceptionTriggered = true;
      Assert.assertTrue(ex.getCause().getMessage().contains("CONTAINER_01200"));
    }
    Assert.assertTrue(exceptionTriggered);
  }

  @Test
  public void testGetRules() {
    Response r = target("/v1/pipeline/myPipeline/rules").queryParam("rev", "tag")
      .request().get();
    Assert.assertNotNull(r);
    RuleDefinitionsJson result = r.readEntity(RuleDefinitionsJson.class);
    Assert.assertEquals(3, result.getMetricsRuleDefinitions().size());
    Assert.assertEquals(3, result.getDataRuleDefinitions().size());

    // test get pipeline rules for which user doesn't have read permission
    boolean exceptionTriggered = false;
    try {
      r = target("/v1/pipeline/noPerm/rules")
          .queryParam("rev", "tag")
          .request().get();
    } catch (Exception ex) {
      exceptionTriggered = true;
      Assert.assertTrue(ex.getCause().getMessage().contains("CONTAINER_01200"));
    }
    Assert.assertTrue(exceptionTriggered);
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
      bindFactory(AclStoreTestInjector.class).to(AclStoreTask.class);
      bindFactory(TestUtil.StageLibraryTestInjector.class).to(StageLibraryTask.class);
      bindFactory(TestUtil.PrincipalTestInjector.class).to(Principal.class);
      bindFactory(TestUtil.URITestInjector.class).to(URI.class);
      bindFactory(RuntimeInfoTestInjector.class).to(RuntimeInfo.class);
      bindFactory(ManagerTestInjector.class).to(Manager.class);
      bindFactory(TestUtil.UserGroupManagerTestInjector.class).to(UserGroupManager.class);
    }
  }

  static PipelineStoreTask pipelineStore;

  static class PipelineStoreTestInjector implements Factory<PipelineStoreTask> {

    public PipelineStoreTestInjector() {
    }

    @Singleton
    @Override
    public PipelineStoreTask provide() {

      com.streamsets.datacollector.util.TestUtil.captureStagesForProductionRun();
      long timestamp = System.currentTimeMillis();
      pipelineStore = Mockito.mock(PipelineStoreTask.class);
      try {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("labels", ImmutableList.of("label1"));

        PipelineInfo pipeline1 = new PipelineInfo(
            "name1",
            "name1",
            "description",
            new java.util.Date(0),
            new java.util.Date(0),
            "user1",
            "lastModifier",
            "1", UUID.randomUUID(),
            true,
            metadata,
            "x",
            "y"
        );
        PipelineInfo pipeline2 = new PipelineInfo(
            "name2",
            "label",
            "description",
            new java.util.Date(0),
            new java.util.Date(0),
            "user1",
            "lastModifier",
            "1", UUID.randomUUID(),
            true,
            metadata,
            "x",
            "y"
        );
        PipelineInfo pipeline3 = new PipelineInfo(
            "name3",
            "label",
            "description",
            new java.util.Date(0),
            new java.util.Date(0),
            "user1",
            "lastModifier",
            "1",
            UUID.randomUUID(),
            true,
            null,
            "x",
            "y"
        );

        PipelineInfo readOnly = new PipelineInfo(
            "readOnly",
            "label",
            "description",
            new java.util.Date(0),
            new java.util.Date(0),
            "user2",
            "lastModifier",
            "1",
            UUID.randomUUID(),
            true,
            null,
            "x",
            "y"
        );

        PipelineInfo readWriteOnly = new PipelineInfo(
            "readWriteOnly",
            "label",
            "description",
            new java.util.Date(0),
            new java.util.Date(0),
            "user2",
            "lastModifier",
            "1",
            UUID.randomUUID(),
            true,
            null,
            "x",
            "y"
        );

        PipelineInfo readWriteExecute = new PipelineInfo(
            "readWriteExecute",
            "label",
            "description",
            new java.util.Date(0),
            new java.util.Date(0),
            "user2",
            "lastModifier",
            "1",
            UUID.randomUUID(),
            true,
            null,
            "x",
            "y"
        );

        PipelineInfo noPerm = new PipelineInfo(
            "noPerm",
            "label",
            "description",
            new java.util.Date(0),
            new java.util.Date(0),
            "user2",
            "lastModifier",
            "1",
            UUID.randomUUID(),
            true,
            null,
            "x",
            "y"
        );

        Mockito.when(pipelineStore.getPipelines())
            .thenReturn(ImmutableList.of(
                pipeline1,
                pipeline2,
                pipeline3,
                readOnly,
                readWriteOnly,
                readWriteExecute,
                noPerm
            ));

        Mockito.when(pipelineStore.getInfo(Matchers.matches("xyz|myPipeline|newFromImport"))).thenReturn(
            new PipelineInfo(
                "xyz",
                "label",
                "xyz description",
                new java.util.Date(0),
                new java.util.Date(0),
                "xyz creator",
                "xyz lastModifier",
                "1", UUID.randomUUID(),
                true,
                null,
                "x",
                "y"
            )
        );
        Mockito.when(pipelineStore.getInfo("name1")).thenReturn(pipeline1);
        Mockito.when(pipelineStore.getInfo("name2")).thenReturn(pipeline2);
        Mockito.when(pipelineStore.getInfo("name3")).thenReturn(pipeline3);
        Mockito.when(pipelineStore.getInfo("readOnly")).thenReturn(readOnly);
        Mockito.when(pipelineStore.getInfo("readWriteOnly")).thenReturn(readWriteOnly);
        Mockito.when(pipelineStore.getInfo("readWriteExecute")).thenReturn(readWriteExecute);
        Mockito.when(pipelineStore.getInfo("noPerm")).thenReturn(noPerm);

        Mockito.when(pipelineStore.getHistory("xyz")).thenReturn(ImmutableList.of(
          new com.streamsets.datacollector.store.PipelineRevInfo(new PipelineInfo("xyz","label",
            "xyz description", new java.util.Date(0), new java.util.Date(0), "xyz creator",
                "xyz lastModifier", "1", UUID.randomUUID(), true, null, "x", "y"))));
        Mockito.when(pipelineStore.load(Matchers.matches("xyz|myPipeline|newFromImport|readOnly"), Mockito.anyString()))
            .thenReturn(MockStages.createPipelineConfigurationSourceProcessorTarget());
        Mockito.when(pipelineStore.load(Matchers.matches("abc|def"), Matchers.matches("0"))).thenReturn(
            MockStages.createPipelineConfigurationWithLabels(new ArrayList<String>()));
        Mockito.when(pipelineStore.create("user1", "myPipeline", "myPipeline", "my description", false, false)).thenReturn(
          MockStages.createPipelineConfigurationSourceProcessorTarget());
        Mockito.doNothing().when(pipelineStore).delete("myPipeline");
        Mockito.doThrow(new PipelineStoreException(ContainerError.CONTAINER_0200, "xyz"))
            .when(pipelineStore).delete("xyz");
        Mockito.when(pipelineStore.save(
            Matchers.anyString(), Matchers.anyString(), Matchers.anyString(), Matchers.anyString(),
            (com.streamsets.datacollector.config.PipelineConfiguration)Matchers.any())).thenReturn(
          MockStages.createPipelineConfigurationSourceProcessorTarget());
        Mockito.when(pipelineStore.save(
            Matchers.anyString(), Matchers.matches("abc|def"), Matchers.matches("0"), Matchers.anyString(),
            (com.streamsets.datacollector.config.PipelineConfiguration)Matchers.any())).thenReturn(
            MockStages.createPipelineConfigurationWithLabels(Arrays.asList("foo", "bar")));

        List<MetricsRuleDefinitionJson> metricsRuleDefinitionJsons = new ArrayList<>();
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
          Mockito.when(pipelineStore.retrieveRules("xyz", "1")).thenReturn(rules.getRuleDefinitions());
          Mockito.when(pipelineStore.retrieveRules("newFromImport", "1")).thenReturn(rules.getRuleDefinitions());
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

        Mockito.when(pipelineStore.create("user1", "newFromImport", "newFromImport",null, false, false)).thenReturn(
            MockStages.createPipelineConfigurationSourceProcessorTarget());

      } catch (com.streamsets.datacollector.util.PipelineException e) {
        LOG.debug("Ignoring exception", e);
      }
      return pipelineStore;
    }

    @Override
    public void dispose(PipelineStoreTask pipelineStore) {
    }
  }


  static AclStoreTask aclStore;

  static class AclStoreTestInjector implements Factory<AclStoreTask> {

    public AclStoreTestInjector() {
    }

    @Singleton
    @Override
    public AclStoreTask provide() {
      aclStore = new FileAclStoreTask (
          Mockito.mock(RuntimeInfo.class),
          pipelineStore,
          new LockCache<String>(),
          Mockito.mock(UserGroupManager.class)
      )  {

        @Override
        public Acl saveAcl(String pipelineName, Acl acl) throws PipelineStoreException {
          return null;
        }

        @Override
        public Acl getAcl(String pipelineName) throws PipelineException {
          Acl user1Acl = createAcl(
              "pipelineId",
              ResourceType.PIPELINE,
              System.currentTimeMillis(),
              "user1"
          );

          Acl readOnlyAcl = createAcl(
              "pipelineId",
              ResourceType.PIPELINE,
              System.currentTimeMillis(),
              "user2"
          );
          readOnlyAcl.getPermissions().add(
              new Permission(
                  "user1",
                  SubjectType.USER,
                  "user2",
                  System.currentTimeMillis(),
                  ImmutableList.of(Action.READ)
              )
          );

          Acl readWriteOnlyAcl = createAcl(
              "pipelineId",
              ResourceType.PIPELINE,
              System.currentTimeMillis(),
              "user2"
          );
          readWriteOnlyAcl.getPermissions().add(
              new Permission(
                  "user1",
                  SubjectType.USER,
                  "user2",
                  System.currentTimeMillis(),
                  ImmutableList.of(Action.READ, Action.WRITE)
              )
          );

          Acl readWriteExecuteAcl = createAcl(
              "pipelineId",
              ResourceType.PIPELINE,
              System.currentTimeMillis(),
              "user2"
          );
          readWriteExecuteAcl.getPermissions().add(
              new Permission(
                  "user1",
                  SubjectType.USER,
                  "user2",
                  System.currentTimeMillis(),
                  ImmutableList.of(Action.READ, Action.WRITE, Action.EXECUTE)
              )
          );

          Acl noPermAcl = createAcl(
              "pipelineId",
              ResourceType.PIPELINE,
              System.currentTimeMillis(),
              "user2"
          );

          switch (pipelineName) {
            case "readOnly":
              return readOnlyAcl;
            case "readWriteOnly":
              return readWriteOnlyAcl;
            case "readWriteExecute":
              return readWriteExecuteAcl;
            case "noPermAcl":
              return noPermAcl;
            case "name1":
            case "name2":
            case "name3":
            case "xyz":
            case "myPipeline":
            case "newFromImport":
            case "myPipeline1":
            case "myPipeline2":
            case "abc":
            case "def":
              return user1Acl;
          }

          return null;
        }
      };

      return aclStore;
    }

    @Override
    public void dispose(AclStoreTask aclStoreTask) {
    }
  }

  static boolean isACLEnabled = true;

  public static class RuntimeInfoTestInjector implements Factory<RuntimeInfo> {
    @Singleton
    @Override
    public RuntimeInfo provide() {
      RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
      Mockito.when(runtimeInfo.isAclEnabled()).thenReturn(isACLEnabled);
      return runtimeInfo;
    }

    @Override
    public void dispose(RuntimeInfo runtimeInfo) {
    }
  }


  static Manager manager;

  static class ManagerTestInjector implements Factory<Manager> {

    public ManagerTestInjector() {
    }

    @Singleton
    @Override
    public Manager provide() {
      manager = Mockito.mock(Manager.class);
      try {
        PipelineState pipelineState1 = new PipelineStateImpl(
            "user",
            "name1",
            "1",
            PipelineStatus.RUNNING,
            "message",
            -1,
            new HashMap<String, Object>(),
            ExecutionMode.STANDALONE,
            "",
            -1,
            -1
        );
        Mockito.when(manager.getPipelineState("name1", "1")).thenReturn(pipelineState1);

        PipelineState pipelineState2 = new PipelineStateImpl(
            "user",
            "name2",
            "1",
            PipelineStatus.START_ERROR,
            "message",
            -1,
            new HashMap<String, Object>(),
            ExecutionMode.STANDALONE,
            "",
            -1,
            -1
        );
        Mockito.when(manager.getPipelineState("name2", "1")).thenReturn(pipelineState2);

        PipelineState pipelineState3 = new PipelineStateImpl(
            "user",
            "name3",
            "1",
            PipelineStatus.RUN_ERROR,
            "message",
            -1,
            new HashMap<String, Object>(),
            ExecutionMode.STANDALONE,
            "",
            -1,
            -1
        );
        Mockito.when(manager.getPipelineState("name3", "1")).thenReturn(pipelineState3);
      } catch (Exception e) {
        LOG.debug("Ignoring exception", e);
      }
      return manager;
    }

    @Override
    public void dispose(Manager manager) {
    }
  }


  @Test
  @SuppressWarnings("unchecked")
  public void testUiInfo() throws Exception {
    PipelineConfiguration conf = MockStages.createPipelineConfigurationSourceProcessorTarget();
    conf.getUiInfo().put("a", "A");

    Response response = target("/v1/pipeline/myPipeline/uiInfo")
        .request()
        .post(Entity.json(Collections.EMPTY_MAP));
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(0, response.getLength());

    Mockito.verify(pipelineStore, Mockito.times(1)).saveUiInfo(Mockito.anyString(), Mockito.anyString(),
                                                               Mockito.anyMap());

  }

  @Test
  public void testExportPipeline() {
    Response response = target("/v1/pipeline/xyz/export").queryParam("rev", "1").request().get();
    PipelineEnvelopeJson pipelineEnvelope = response.readEntity(PipelineEnvelopeJson.class);
    Assert.assertNotNull(pipelineEnvelope);
    Assert.assertNotNull(pipelineEnvelope.getPipelineConfig());
    Assert.assertNotNull(pipelineEnvelope.getPipelineRules());
    Assert.assertNull(pipelineEnvelope.getLibraryDefinitions());
  }

  @Test
  public void testExportPipelineWithDefinitions() {
    Response response = target("/v1/pipeline/xyz/export").queryParam("rev", "1")
        .queryParam("includeLibraryDefinitions", true).request().get();
    // Reading as  PipelineEnvelopeJson ignores the definitions, so reading as Map
    Map pipelineEnvelope = response.readEntity(Map.class);
    Assert.assertNotNull(pipelineEnvelope);

    Assert.assertTrue(pipelineEnvelope.containsKey("pipelineConfig"));
    Assert.assertNotNull(pipelineEnvelope.get("pipelineConfig"));

    Assert.assertTrue(pipelineEnvelope.containsKey("pipelineRules"));
    Assert.assertNotNull(pipelineEnvelope.get("pipelineRules"));

    Assert.assertTrue(pipelineEnvelope.containsKey("libraryDefinitions"));
    Assert.assertNotNull(pipelineEnvelope.get("libraryDefinitions"));
  }

  @Test
  public void testImportPipeline() {
    Response response = target("/v1/pipeline/xyz/export").queryParam("rev", "1").request().get();
    PipelineEnvelopeJson pipelineEnvelope = response.readEntity(PipelineEnvelopeJson.class);

    response = target("/v1/pipeline/newFromImport/import")
        .queryParam("rev", "1")
        .request()
        .post(Entity.json(pipelineEnvelope));
    pipelineEnvelope = response.readEntity(PipelineEnvelopeJson.class);

    Assert.assertNotNull(pipelineEnvelope);
    Assert.assertNotNull(pipelineEnvelope.getPipelineConfig());
    Assert.assertNotNull(pipelineEnvelope.getPipelineRules());
    Assert.assertNull(pipelineEnvelope.getLibraryDefinitions());
  }

  @Test
  public void testAddLabelsToPipelines() throws PipelineStoreException {
    Response response = target("/v1/pipelines/addLabels").request()
        .post(Entity.json("{\"labels\": [\"foo\", \"bar\"], \"pipelineNames\": [\"abc\", \"def\", \"nonExistent\"]}"));

    MultiStatusResponseJson<String> multiStatusResponse = response.readEntity(MultiStatusResponseJson.class);
    Assert.assertEquals(207, response.getStatusInfo().getStatusCode());

    List<String> successEntities = multiStatusResponse.getSuccessEntities();
    Assert.assertEquals(Arrays.asList("abc", "def"), successEntities);

    List<String> errorMessages = multiStatusResponse.getErrorMessages();
    Assert.assertEquals(Arrays.asList("Failed adding labels [foo, bar] to pipeline: nonExistent. Error: null"),
        errorMessages);
  }
}
