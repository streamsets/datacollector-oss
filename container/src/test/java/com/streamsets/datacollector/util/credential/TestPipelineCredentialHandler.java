/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.util.credential;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineDefinition;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.creation.PipelineBean;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.creation.PipelineStageBeans;
import com.streamsets.datacollector.creation.ServiceConfigurationBuilder;
import com.streamsets.datacollector.creation.StageBean;
import com.streamsets.datacollector.credential.CredentialEL;
import com.streamsets.datacollector.credential.CredentialStoresTask;
import com.streamsets.datacollector.definition.ServiceDefinitionExtractor;
import com.streamsets.datacollector.definition.StageDefinitionExtractor;
import com.streamsets.datacollector.runner.preview.StageConfigurationBuilder;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
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
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.credential.ManagedCredentialStore;
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
import java.util.Optional;
import java.util.UUID;

public class TestPipelineCredentialHandler {
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


  static class MyManagedCredentialValue implements CredentialValue {
    private final String value;

    MyManagedCredentialValue(String value) {
      this.value = value;
    }

    @Override
    public String get() throws StageException {
      return value;
    }
  }

  static class MyManagedCredentialStore implements ManagedCredentialStore {
    private final Map<String, String> credentialStoreMap = new HashMap<>();

    @Override
    public void store(List<String> groups, String name, String credentialValue) throws StageException {
      credentialStoreMap.put(name, credentialValue);
    }

    @Override
    public void delete(String name) throws StageException {
      credentialStoreMap.remove(name);
    }

    @Override
    public List<String> getNames() throws StageException {
      return new ArrayList<>(credentialStoreMap.keySet());
    }

    @Override
    public List<ConfigIssue> init(Context context) {
      return Collections.emptyList();
    }

    @Override
    public CredentialValue get(String group, String name, String credentialStoreOptions) throws StageException {
      return new MyManagedCredentialValue(credentialStoreMap.get(name));
    }

    @Override
    public void destroy() {
      credentialStoreMap.clear();
    }
  }

  private MyManagedCredentialStore myManagedCredentialStore;
  private StageLibraryTask stageLibraryTask;
  private CredentialStoresTask credentialStoresTask;
  private com.streamsets.datacollector.util.Configuration configuration;
  private PipelineCredentialHandler pipelineCredentialHandler;

  @Before
  public void init() {
    myManagedCredentialStore = Mockito.spy(new MyManagedCredentialStore());

    configuration = Mockito.mock(com.streamsets.datacollector.util.Configuration.class);
    credentialStoresTask = Mockito.mock(CredentialStoresTask.class);

    Mockito.when(credentialStoresTask.getDefaultManagedCredentialStore()).thenReturn(myManagedCredentialStore);
    Mockito.when(
        configuration.get(Mockito.eq(CredentialStoresTask.MANAGED_DEFAULT_CREDENTIAL_STORE_CONFIG), Mockito.anyString())
    ).thenReturn("streamsets");
    CredentialEL.setCredentialStores(ImmutableMap.of("streamsets", myManagedCredentialStore, "cs", myManagedCredentialStore));
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    StageDefinition sourceDef = StageDefinitionExtractor.get().extract(libraryDef, MySource.class, "");
    StageDefinition targetDef = StageDefinitionExtractor.get().extract(libraryDef, MyTarget.class, "");
    StageDefinition lifecycleTargetDef = StageDefinitionExtractor.get().extract(libraryDef, LifecycleEventMyTarget.class, "");
    StageDefinition errorStageDef = StageDefinitionExtractor.get().extract(libraryDef, ErrorMyTarget.class, "");
    StageDefinition aggStageDef = StageDefinitionExtractor.get().extract(libraryDef, AggregatingMyTarget.class, "");
    ServiceDefinition serviceDef = ServiceDefinitionExtractor.get().extract(libraryDef, CoolService.class);
    stageLibraryTask = Mockito.mock(StageLibraryTask.class);
    Mockito.when(stageLibraryTask.getPipeline()).thenReturn(PipelineDefinition.getPipelineDef());
    Mockito.when(stageLibraryTask.getStage(Mockito.eq("default"), Mockito.eq("s"), Mockito.eq(false)))
        .thenReturn(sourceDef);
    Mockito.when(stageLibraryTask.getStage(Mockito.eq("default"), Mockito.eq("t"), Mockito.eq(false)))
        .thenReturn(targetDef);
    Mockito.when(stageLibraryTask.getStage(Mockito.eq("default"), Mockito.eq("e"), Mockito.eq(false)))
        .thenReturn(errorStageDef);
    Mockito.when(stageLibraryTask.getStage(Mockito.eq("default"), Mockito.eq("a"), Mockito.eq(false)))
        .thenReturn(aggStageDef);
    Mockito.when(stageLibraryTask.getStage(Mockito.eq("default"), Mockito.eq("lifecycle"), Mockito.eq(false)))
        .thenReturn(lifecycleTargetDef);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    Mockito.when(stageLibraryTask.getServiceDefinition(Mockito.eq(Runnable.class), Mockito.anyBoolean())).thenReturn(serviceDef);

  }

  @After
  public void destroy() {
    CredentialEL.setCredentialStores(null);
    myManagedCredentialStore.destroy();
  }

  @Test
  public void testDecryptPlainTextCredentials() throws StageException {
    final String PIPELINE_ID = "pipelineId";
    final String AMAZON_ACCESS_KEY = "__sdcEmrConnection.awsConfig.awsAccessKeyId";
    final String SOURCE_PASSWORD = "/si__password1";
    final String SOURCE_COMPLEX_FIELD_PASSWORD = "/si__complexField[0][password1]";
    final List<String> CONFIGS_TO_HANDLE =
        ImmutableList.of(AMAZON_ACCESS_KEY, SOURCE_PASSWORD, SOURCE_COMPLEX_FIELD_PASSWORD);
    CONFIGS_TO_HANDLE.forEach(c ->
        myManagedCredentialStore.store(
            CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST,
            CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + PIPELINE_ID + c,
            "secret"
        )
    );
    pipelineCredentialHandler = PipelineCredentialHandler.getDecrypter(stageLibraryTask, credentialStoresTask, configuration);

    List<Map<String, Object>> constants = new ArrayList<>();
    Map<String, Object> constantValue = new LinkedHashMap<>();
    constantValue.put("key", "MEMORY_LIMIT");
    constantValue.put("value", 1000);
    constants.add(constantValue);
    List<Config> pipelineConfigs = ImmutableList.of(
        new Config("executionMode", ExecutionMode.CLUSTER_BATCH.name()),
        new Config("constants", constants),
        new Config("sdcEmrConnection.awsConfig.awsAccessKeyId",
            "${credential:get('streamsets','all','" +   CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + PIPELINE_ID + AMAZON_ACCESS_KEY + "')}" ),
        new Config("sdcEmrConnection.awsConfig.awsSecretAccessKey", "secretKey plain text")
    );

    Map<String, Object> complexFieldValue = new HashMap<>();
    complexFieldValue.put("beanInt", 4);
    complexFieldValue.put("password1", "${credential:get('streamsets','all','" +   CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + PIPELINE_ID + SOURCE_PASSWORD + "')}");
    complexFieldValue.put("password2", "${'a'}");
    complexFieldValue.put("password3", "b");

    StageConfiguration sourceConf = new StageConfigurationBuilder("si", "s")
        .withConfig(
            new Config("list", ImmutableList.of("S")),
            new Config("password1",
                "${credential:get('streamsets','all','" +   CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + PIPELINE_ID + SOURCE_COMPLEX_FIELD_PASSWORD + "')}"
            ),
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
        PIPELINE_ID,
        UUID.randomUUID(),
        "label",
        "D",
        pipelineConfigs,
        Collections.emptyMap(),
        ImmutableList.of(sourceConf, targetConf),
        errorStageConf,
        aggStageConf,
        ImmutableList.of(startConf),
        ImmutableList.of(stopConf)
    );

    pipelineCredentialHandler.handlePipelineConfigCredentials(pipelineConf);

    //Check the config values
    pipelineConf.getConfiguration().forEach(c -> {
      if (c.getName().equals("sdcEmrConnection.awsConfig.awsAccessKeyId")) {
        Assert.assertEquals("secret", c.getValue());
      }
    });

    Optional<StageConfiguration> sourceConfig =
        pipelineConf.getStages().stream().filter(stageConfiguration -> stageConfiguration.getInstanceName().equals("si")).findFirst();
    Assert.assertTrue(sourceConfig.isPresent());
    sourceConfig.ifPresent(s -> {
      s.getConfiguration().forEach(c -> {
        if (c.getName().equals("password1")) {
          Assert.assertEquals("secret", c.getValue());
        } else if (c.getName().equals("complexField")) {
          List<Map<String, Object>> complexFieldList = (List<Map<String, Object>>) c.getValue();
          Map<String, Object> complexField = complexFieldList.get(0);
          Object complexPasswordFieldValue = complexField.get("password1");
          Assert.assertEquals("secret", complexPasswordFieldValue);
        }
      });
    });
  }

  @Test
  public void testAutoEncryptingCredentials() {
    final String PIPELINE_ID = "pipelineId";
    final String AMAZON_ACCESS_KEY = "__sdcEmrConnection.awsConfig.awsAccessKeyId";
    final String AMAZON_SECRET_KEY = "__sdcEmrConnection.awsConfig.awsSecretAccessKey";

    final String SOURCE_PASSWORD = "/si__password1";
    final String SOURCE_COMPLEX_FIELD_PASSWORD = "/si__complexField[0][password1]";

    pipelineCredentialHandler = PipelineCredentialHandler.getEncrypter(stageLibraryTask, credentialStoresTask, configuration);

    List<Map<String, Object>> constants = new ArrayList<>();
    Map<String, Object> constantValue = new LinkedHashMap<>();
    constantValue.put("key", "MEMORY_LIMIT");
    constantValue.put("value", 1000);
    constants.add(constantValue);
    List<Config> pipelineConfigs = ImmutableList.of(
        new Config("executionMode", ExecutionMode.CLUSTER_BATCH.name()),
        new Config("constants", constants),
        new Config("sdcEmrConnection.awsConfig.awsAccessKeyId", "accessKey plain text"),
        new Config("sdcEmrConnection.awsConfig.awsSecretAccessKey", "secretKey plain text")
    );

    Map<String, Object> complexFieldValue = new HashMap<>();
    complexFieldValue.put("beanInt", 4);
    complexFieldValue.put("password1", "complex field password1 plain text");
    complexFieldValue.put("password2", "${'a'}");
    complexFieldValue.put("password3", "b");

    StageConfiguration sourceConf = new StageConfigurationBuilder("si", "s")
        .withConfig(
            new Config("list", ImmutableList.of("S")),
            new Config("password1", "password1 plain text"),
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
        Collections.emptyMap(),
        ImmutableList.of(sourceConf, targetConf),
        errorStageConf,
        aggStageConf,
        ImmutableList.of(startConf),
        ImmutableList.of(stopConf)
    );
    pipelineCredentialHandler.handlePipelineConfigCredentials(pipelineConf);

    //check configs and credential store
    pipelineConf.getConfiguration().forEach(c -> {
      if (c.getName().equals("sdcEmrConnection.awsConfig.awsAccessKeyId")) {
        Assert.assertEquals("${credential:get('streamsets','all','" + CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + PIPELINE_ID + AMAZON_ACCESS_KEY + "')}", c.getValue());
        myManagedCredentialStore.get(CredentialStoresTask.DEFAULT_SDC_GROUP, CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + PIPELINE_ID + AMAZON_ACCESS_KEY, "accessKey plain text");
      } else if (c.getName().equals("sdcEmrConnection.awsConfig.awsSecretAccessKey")) {
        Assert.assertEquals("${credential:get('streamsets','all','" + CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + PIPELINE_ID + AMAZON_SECRET_KEY + "')}", c.getValue());
        myManagedCredentialStore.get(CredentialStoresTask.DEFAULT_SDC_GROUP, CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + PIPELINE_ID + AMAZON_SECRET_KEY, "secretKey plain text");
      }
    });

    Optional<StageConfiguration> sourceConfig =
        pipelineConf.getStages().stream().filter(stageConfiguration -> stageConfiguration.getInstanceName().equals("si")).findFirst();
    Assert.assertTrue(sourceConfig.isPresent());
    sourceConfig.ifPresent(s -> {
      s.getConfiguration().forEach(c -> {
        if (c.getName().equals("password1")) {
          Assert.assertEquals(
              "${credential:get('streamsets','all','" + CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + PIPELINE_ID + SOURCE_PASSWORD + "')}",
              c.getValue()
          );
          myManagedCredentialStore.get(CredentialStoresTask.DEFAULT_SDC_GROUP, CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + PIPELINE_ID + SOURCE_PASSWORD, "password1 plain text");
        } else if (c.getName().equals("complexField")) {
          List<Map<String, Object>> complexFieldList = (List<Map<String, Object>>) c.getValue();
          Map<String, Object> complexField = complexFieldList.get(0);
          Object complexPasswordFieldValue = complexField.get("password1");
          Assert.assertEquals(
              "${credential:get('streamsets','all','" + CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + PIPELINE_ID + SOURCE_COMPLEX_FIELD_PASSWORD + "')}",
              complexPasswordFieldValue
          );
          Assert.assertEquals(
              "complex field password1 plain text",
              myManagedCredentialStore.get(
                  CredentialStoresTask.DEFAULT_SDC_GROUP,
                  CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + PIPELINE_ID + SOURCE_COMPLEX_FIELD_PASSWORD,
                  ""
              ).get()
          );
        }
      });
    });
  }

  @Test
  public void testStripPipelineConfigPlainCredentials() throws StageException {
    myManagedCredentialStore.store(CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST, "g", "secret");
    pipelineCredentialHandler = PipelineCredentialHandler.getPlainTextScrubber(stageLibraryTask);

    List<Map<String, Object>> constants = new ArrayList<>();
    Map<String, Object> constantValue = new LinkedHashMap<>();
    constantValue.put("key", "MEMORY_LIMIT");
    constantValue.put("value", 1000);
    constants.add(constantValue);
    List<Config> pipelineConfigs = ImmutableList.of(
        new Config("executionMode", ExecutionMode.CLUSTER_BATCH.name()),
        new Config("constants", constants),
        new Config("sdcEmrConnection.awsConfig.awsAccessKeyId", "${credential:get('cs','all','g')}"),
        new Config("sdcEmrConnection.awsConfig.awsSecretAccessKey", "secretKey plain text")
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

    pipelineCredentialHandler.handlePipelineConfigCredentials(pipelineConf);

    List<Issue> issues = new ArrayList<>();
    PipelineBean bean = PipelineBeanCreator.get().create(false, stageLibraryTask, pipelineConf, null, null, null, issues);

    Assert.assertNotNull(bean);

    // pipeline configs
    Assert.assertEquals(ExecutionMode.CLUSTER_BATCH, bean.getConfig().executionMode);
    // test pipeline plain credentials
    Assert.assertEquals("secret", bean.getConfig().sdcEmrConnection.awsConfig.awsAccessKeyId.get());
    Assert.assertEquals("", bean.getConfig().sdcEmrConnection.awsConfig.awsSecretAccessKey.get());

    // Origin
    Assert.assertNotNull(bean.getOrigin());
    TestPipelineCredentialHandler.MySource source = (TestPipelineCredentialHandler.MySource) bean.getOrigin().getStage();
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
    TestPipelineCredentialHandler.MyTarget target = (TestPipelineCredentialHandler.MyTarget)targetBean.getStage();
    Assert.assertEquals(ImmutableList.of("T"), target.list);
    Assert.assertEquals(1, targetBean.getServices().size());

    // Aggregating stage
    TestPipelineCredentialHandler.AggregatingMyTarget aggregatingStage = (TestPipelineCredentialHandler.AggregatingMyTarget) bean.getStatsAggregatorStage().getStage();
    Assert.assertEquals(ImmutableList.of("A"), aggregatingStage.list);

    // error stage
    TestPipelineCredentialHandler.ErrorMyTarget errorStage = (TestPipelineCredentialHandler.ErrorMyTarget) bean.getErrorStage().getStage();
    Assert.assertEquals(ImmutableList.of("E"), errorStage.list);

    // Lifecycle events
    Assert.assertEquals(1, bean.getStartEventStages().size());
    TestPipelineCredentialHandler.LifecycleEventMyTarget startStage = (TestPipelineCredentialHandler.LifecycleEventMyTarget)bean.getStartEventStages().get(0).getStage();
    Assert.assertEquals(ImmutableList.of("start-list"), startStage.list);
    Assert.assertEquals(1, bean.getStopEventStages().size());
    TestPipelineCredentialHandler.LifecycleEventMyTarget stopStage = (TestPipelineCredentialHandler.LifecycleEventMyTarget)bean.getStopEventStages().get(0).getStage();
    Assert.assertEquals(ImmutableList.of("stop-list"), stopStage.list);

    // pass runtime parameters
    Map<String, Object> runtimeParameters = ImmutableMap.of("MEMORY_LIMIT", 2000);
    issues = new ArrayList<>();
    bean = PipelineBeanCreator.get().create(false, stageLibraryTask, pipelineConf, null, issues, runtimeParameters, null, null);
    Assert.assertNotNull(bean);
    // pipeline configs
    Assert.assertEquals(ExecutionMode.CLUSTER_BATCH, bean.getConfig().executionMode);

    // Verify duplicate stage bean
    issues = new ArrayList<>();
    PipelineStageBeans duplicate = PipelineBeanCreator.get().duplicatePipelineStageBeans(
        stageLibraryTask,
        bean.getPipelineStageBeans(),
        null,
        Collections.emptyMap(),
        null,
        null,
        issues
    );
    Assert.assertNotNull(duplicate);
    Assert.assertEquals(1, duplicate.size());
    targetBean = stages.getStages().get(0);
    target = (TestPipelineCredentialHandler.MyTarget)targetBean.getStage();
    Assert.assertEquals(ImmutableList.of("T"), target.list);
    Assert.assertEquals(1, targetBean.getServices().size());
  }

}
