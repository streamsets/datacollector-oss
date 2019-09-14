/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.datacollector.creation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineDefinition;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.credential.CredentialEL;
import com.streamsets.datacollector.definition.ServiceDefinitionExtractor;
import com.streamsets.datacollector.definition.StageDefinitionExtractor;
import com.streamsets.datacollector.runner.preview.StageConfigurationBuilder;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.PipelineConfigurationUtil;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.BlobStoreDef;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.MultiValueChooserModel;
import com.streamsets.pipeline.api.PipelineLifecycleStage;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StatsAggregatorStage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.interceptor.BaseInterceptor;
import com.streamsets.pipeline.api.interceptor.Interceptor;
import com.streamsets.pipeline.api.interceptor.InterceptorCreator;
import com.streamsets.pipeline.api.interceptor.InterceptorDef;
import com.streamsets.pipeline.api.service.Service;
import com.streamsets.pipeline.api.service.ServiceDef;
import com.streamsets.pipeline.api.service.ServiceDependency;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@SuppressWarnings("unchecked")
public class TestStrippingPlainCredentials {

  @Before
  public void setup() {
    CredentialStore cs = new CredentialStore() {
      @Override
      public List<ConfigIssue> init(Context context) {
        return null;
      }

      @Override
      public CredentialValue get(String group, String name, String credentialStoreOptions) {
        return () -> "secret";
      }

      @Override
      public void destroy() {

      }
    };
    CredentialEL.setCredentialStores(org.testcontainers.shaded.com.google.common.collect.ImmutableMap.of("cs", cs));
  }

  @After
  public void cleanup() {
    CredentialEL.setCredentialStores(null);
  }

  public enum E { A, B }

  public static class SubBean {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        defaultValue = "A",
        required = true
    )
    public E subBeanEnum;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        defaultValue = "B",
        required = false
    )
    public String subBeanString;


    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.LIST,
        defaultValue = "[\"3\"]",
        required = true
    )
    public List<String> subBeanList;

  }

  public static class Bean {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.NUMBER,
        defaultValue = "1",
        required = false
    )
    public int beanInt;


    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.CREDENTIAL,
        required = true,
        defaultValue = "${credential:get('cs','all','g')}",
        elDefs = CredentialEL.class
    )
    public CredentialValue password1;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.CREDENTIAL,
        required = true,
        defaultValue = "${'a'}",
        elDefs = CredentialEL.class
    )
    public CredentialValue password2;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.CREDENTIAL,
        required = true,
        defaultValue = "b",
        elDefs = CredentialEL.class
    )
    public CredentialValue password3;

    @ConfigDefBean
    public SubBean beanSubBean;

  }

  public static class EValueChooser extends BaseEnumChooserValues<E> {
    public EValueChooser() {
      super(E.class);
    }
  }

  @ServiceDef(
      provides = Runnable.class,
      version = 1,
      label = "The best service ever"
  )
  public static class CoolService implements Service, Runnable {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.NUMBER,
        defaultValue = "1",
        required = false
    )
    public int beanInt;

    @Override
    public List<ConfigIssue> init(Context context) {
      return null;
    }

    @Override
    public void destroy() {

    }

    @Override
    public void run() {

    }
  }

  @StageDef(
      version = 1,
      label = "L",
      onlineHelpRefUrl = "",
      services = @ServiceDependency(service = Runnable.class)
  )
  public static class MySource extends BaseSource {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.LIST,
        defaultValue = "[\"1\"]",
        required = true
    )
    public List<String> list;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.LIST,
        defaultValue = "[\"2\"]",
        required = true,
        evaluation = ConfigDef.Evaluation.EXPLICIT
    )
    public List<String> listExplicit;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MAP,
        defaultValue = "{\"a\" : \"1\"}",
        required = true
    )
    public Map<String, String> map;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MAP,
        defaultValue = "{\"a\" : \"2\"}",
        required = true,
        evaluation = ConfigDef.Evaluation.EXPLICIT
    )
    public Map<String, String> mapExplicit;

    @ConfigDefBean
    public Bean bean;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @ListBeanModel
    public List<Bean> complexField;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true
    )
    public String stringJavaDefault = "Hello";

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.NUMBER,
        required = true
    )
    public int intJavaDefault = 5;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @ValueChooserModel(EValueChooser.class)
    public E enumSJavaDefault = E.B;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @MultiValueChooserModel(EValueChooser.class)
    public List<E> enumMJavaDefault = Arrays.asList(E.A);

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @ValueChooserModel(EValueChooser.class)
    public E enumSNoDefaultAlAll;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.CREDENTIAL,
        required = true,
        defaultValue = "${credential:get('cs','all','g')}",
        elDefs = CredentialEL.class
    )
    public CredentialValue password1;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.CREDENTIAL,
        required = true,
        defaultValue = "${'a'}",
        elDefs = CredentialEL.class
    )
    public CredentialValue password2;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.CREDENTIAL,
        required = true,
        defaultValue = "b",
        elDefs = CredentialEL.class
    )
    public CredentialValue password3;

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  @StageDef(version = 1, label = "L", onlineHelpRefUrl = "")
  public static class MyPushSource extends BasePushSource {

    @Override
    public int getNumberOfThreads() {
      return 2;
    }

    @Override
    public void produce(Map<String, String> offsets, int maxBatchSize) throws StageException {

    }
  }

  @StageDef(
      version = 1,
      label = "L",
      onlineHelpRefUrl = "",
      services = @ServiceDependency(service = CoolService.class)
  )
  public static class MyTarget extends BaseTarget {
    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.LIST,
        defaultValue = "[\"1\"]",
        required = true
    )
    public List<String> list;

    @Override
    public void write(Batch batch) throws StageException {
    }
  }

  @StageDef(version = 1, label = "Lifecycle", onlineHelpRefUrl = "")
  @PipelineLifecycleStage
  public static class LifecycleEventMyTarget extends MyTarget {
  }


  @StageDef(version = 1, label = "L", onlineHelpRefUrl = "")
  @ErrorStage
  public static class ErrorMyTarget extends MyTarget {
  }

  @StageDef(version = 1, label = "A", onlineHelpRefUrl = "")
  @StatsAggregatorStage
  public static class AggregatingMyTarget extends MyTarget {
  }

  public static class MyDefaultInterceptorCreator implements InterceptorCreator {
    @Override
    public Interceptor create(Context context) {
      return new MyInterceptor(context.getStageType().name() + " " + context.getInterceptorType().name());
    }

    @Override
    public List<BlobStoreDef> blobStoreResource(BaseContext context){
      return new ArrayList<>();
    }
  }

  @InterceptorDef(
      version = 1,
      creator = MyDefaultInterceptorCreator.class
  )
  public static class MyInterceptor extends BaseInterceptor {
    public final String str;
    public MyInterceptor(String str) {
      this.str = str;
    }

    @Override
    public List<Record> intercept(List<Record> records) {
      return records;
    }
  }

  @Test
  public void testStripPipelineConfigPlainCredentials() throws StageException {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    StageDefinition sourceDef = StageDefinitionExtractor.get().extract(libraryDef, MySource.class, "");
    StageDefinition targetDef = StageDefinitionExtractor.get().extract(libraryDef, MyTarget.class, "");
    StageDefinition lifecycleTargetDef = StageDefinitionExtractor.get().extract(libraryDef, LifecycleEventMyTarget.class, "");
    StageDefinition errorStageDef = StageDefinitionExtractor.get().extract(libraryDef, ErrorMyTarget.class, "");
    StageDefinition aggStageDef = StageDefinitionExtractor.get().extract(libraryDef, AggregatingMyTarget.class, "");
    ServiceDefinition serviceDef = ServiceDefinitionExtractor.get().extract(libraryDef, CoolService.class);
    StageLibraryTask library = Mockito.mock(StageLibraryTask.class);
    Mockito.when(library.getPipeline()).thenReturn(PipelineDefinition.getPipelineDef());
    Mockito.when(library.getStage(Mockito.eq("default"), Mockito.eq("s"), Mockito.eq(false)))
        .thenReturn(sourceDef);
    Mockito.when(library.getStage(Mockito.eq("default"), Mockito.eq("t"), Mockito.eq(false)))
        .thenReturn(targetDef);
    Mockito.when(library.getStage(Mockito.eq("default"), Mockito.eq("e"), Mockito.eq(false)))
        .thenReturn(errorStageDef);
    Mockito.when(library.getStage(Mockito.eq("default"), Mockito.eq("a"), Mockito.eq(false)))
        .thenReturn(aggStageDef);
    Mockito.when(library.getStage(Mockito.eq("default"), Mockito.eq("lifecycle"), Mockito.eq(false)))
        .thenReturn(lifecycleTargetDef);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    Mockito.when(library.getServiceDefinition(Mockito.eq(Runnable.class), Mockito.anyBoolean())).thenReturn(serviceDef);

    List<Map<String, Object>> constants = new ArrayList<>();
    Map<String, Object> constantValue = new LinkedHashMap<>();
    constantValue.put("key", "MEMORY_LIMIT");
    constantValue.put("value", 1000);
    constants.add(constantValue);
    List<Config> pipelineConfigs = ImmutableList.of(
        new Config("executionMode", ExecutionMode.CLUSTER_BATCH.name()),
        new Config("constants", constants),
        new Config("amazonEMRConfig.accessKey", "${credential:get('cs','all','g')}"),
        new Config("amazonEMRConfig.secretKey", "secretKey plain text")
    );

    Map<String, Object> complexFieldValue = new HashMap<>();
    complexFieldValue.put("beanInt", 4);
    complexFieldValue.put("password1", "${credential:get('cs','all','g')}");
    complexFieldValue.put("password2", "${'a'}");
    complexFieldValue.put("password3", "b");

    StageConfiguration sourceConf = new StageConfigurationBuilder("si", "s")
        .withConfig(
            new Config("list", ImmutableList.of("S")),
            new Config("password1", "${credential:get('cs','all','g')}"),
            new Config("password2", "${'a'}"),
            new Config("password3", "b"),
            new Config("complexField", ImmutableList.of(complexFieldValue))
        )
        .build();
    StageConfiguration targetConf = new StageConfigurationBuilder("si", "t")
        .withConfig(new Config("list", ImmutableList.of("T")))
        .withServices(new ServiceConfigurationBuilder()
            .withService(Runnable.class)
            .build())
        .build();
    StageConfiguration errorStageConf = new StageConfigurationBuilder("ei", "e")
        .withConfig(new Config("list", ImmutableList.of("E")))
        .build();
    StageConfiguration aggStageConf = new StageConfigurationBuilder("ai", "a")
        .withConfig(new Config("list", ImmutableList.of("A")))
        .build();
    StageConfiguration startConf = new StageConfigurationBuilder("start", "lifecycle")
        .withConfig(new Config("list", ImmutableList.of("start-list")))
        .build();
    StageConfiguration stopConf = new StageConfigurationBuilder("stop", "lifecycle")
        .withConfig(new Config("list", ImmutableList.of("stop-list")))
        .build();

    PipelineConfiguration pipelineConf = new PipelineConfiguration(
        1,
        PipelineConfigBean.VERSION,
        "pipelineId",
        UUID.randomUUID(),
        "label",
        "D",
        pipelineConfigs,
        Collections.EMPTY_MAP,
        ImmutableList.of(sourceConf, targetConf),
        errorStageConf,
        aggStageConf,
        ImmutableList.of(startConf),
        ImmutableList.of(stopConf)
    );


    PipelineConfigurationUtil.stripPipelineConfigPlainCredentials(pipelineConf, library);


    List<Issue> issues = new ArrayList<>();
    PipelineBean bean = PipelineBeanCreator.get().create(false, library, pipelineConf, null, issues);

    Assert.assertNotNull(bean);

    // pipeline configs
    Assert.assertEquals(ExecutionMode.CLUSTER_BATCH, bean.getConfig().executionMode);
    // test pipeline plain credentials
    Assert.assertEquals("secret", bean.getConfig().amazonEMRConfig.accessKey.get());
    Assert.assertEquals("", bean.getConfig().amazonEMRConfig.secretKey.get());

    // Origin
    Assert.assertNotNull(bean.getOrigin());
    MySource source = (MySource) bean.getOrigin().getStage();
    Assert.assertEquals(ImmutableList.of("S"), source.list);
    // test credentials config value
    Assert.assertEquals("secret", source.password1.get());
    Assert.assertEquals("a", source.password2.get());
    Assert.assertEquals("", source.password3.get());

    Assert.assertEquals(1, source.complexField.size());
    Assert.assertEquals(4, source.complexField.get(0).beanInt);
    Assert.assertEquals("secret", source.complexField.get(0).password1.get());
    Assert.assertEquals("a", source.complexField.get(0).password2.get());
    Assert.assertEquals("", source.complexField.get(0).password3.get());

    // Target
    Assert.assertEquals(1, bean.getPipelineStageBeans().size());
    PipelineStageBeans stages = bean.getPipelineStageBeans();
    Assert.assertEquals(1, stages.getStages().size());
    StageBean targetBean = stages.getStages().get(0);
    MyTarget target = (MyTarget)targetBean.getStage();
    Assert.assertEquals(ImmutableList.of("T"), target.list);
    Assert.assertEquals(1, targetBean.getServices().size());

    // Aggregating stage
    AggregatingMyTarget aggregatingStage = (AggregatingMyTarget) bean.getStatsAggregatorStage().getStage();
    Assert.assertEquals(ImmutableList.of("A"), aggregatingStage.list);

    // error stage
    ErrorMyTarget errorStage = (ErrorMyTarget) bean.getErrorStage().getStage();
    Assert.assertEquals(ImmutableList.of("E"), errorStage.list);

    // Lifecycle events
    Assert.assertEquals(1, bean.getStartEventStages().size());
    LifecycleEventMyTarget startStage = (LifecycleEventMyTarget)bean.getStartEventStages().get(0).getStage();
    Assert.assertEquals(ImmutableList.of("start-list"), startStage.list);
    Assert.assertEquals(1, bean.getStopEventStages().size());
    LifecycleEventMyTarget stopStage = (LifecycleEventMyTarget)bean.getStopEventStages().get(0).getStage();
    Assert.assertEquals(ImmutableList.of("stop-list"), stopStage.list);

    // pass runtime parameters
    Map<String, Object> runtimeParameters = ImmutableMap.of("MEMORY_LIMIT", 2000);
    issues = new ArrayList<>();
    bean = PipelineBeanCreator.get().create(false, library, pipelineConf, null, issues, runtimeParameters);
    Assert.assertNotNull(bean);
    // pipeline configs
    Assert.assertEquals(ExecutionMode.CLUSTER_BATCH, bean.getConfig().executionMode);

    // Verify duplicate stage bean
    issues = new ArrayList<>();
    PipelineStageBeans duplicate = PipelineBeanCreator.get().duplicatePipelineStageBeans(
        library,
        bean.getPipelineStageBeans(),
        null,
        Collections.emptyMap(),
        issues
    );
    Assert.assertNotNull(duplicate);
    Assert.assertEquals(1, duplicate.size());
    targetBean = stages.getStages().get(0);
    target = (MyTarget)targetBean.getStage();
    Assert.assertEquals(ImmutableList.of("T"), target.list);
    Assert.assertEquals(1, targetBean.getServices().size());
  }

}
