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

import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.CredentialStoreDefinition;
import com.streamsets.datacollector.config.PipelineDefinition;
import com.streamsets.datacollector.config.PipelineFragmentDefinition;
import com.streamsets.datacollector.config.PipelineRulesDefinition;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.credential.CredentialStoresTask;
import com.streamsets.datacollector.el.ElConstantDefinition;
import com.streamsets.datacollector.el.ElFunctionDefinition;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.restapi.bean.UserJson;
import com.streamsets.datacollector.runner.StageDefinitionBuilder;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.credential.ManagedCredentialStore;
import org.glassfish.hk2.api.Factory;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import javax.inject.Singleton;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(TestUtil.class);

  private static final String PIPELINE_NAME = "myPipeline";
  private static final String PIPELINE_REV = "2.0";
  private static final String DEFAULT_PIPELINE_REV = "0";
  private static final String SNAPSHOT_NAME = "snapshot";

  /**
   * Mock source implementation
   */
  public static class TSource extends BaseSource {
    public boolean inited;
    public boolean destroyed;

    @Override
    protected List<ConfigIssue> init() {
      List<ConfigIssue> issues = super.init();
      inited = true;
      return issues;
    }

    @Override
    public void destroy() {
      destroyed = true;
    }

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  /**
   * Mock target implementation
   */
  public static class TTarget extends BaseTarget {
    public boolean inited;
    public boolean destroyed;

    @Override
    protected List<ConfigIssue> init() {
      List<ConfigIssue> issues = super.init();
      inited = true;
      return issues;
    }

    @Override
    public void destroy() {
      destroyed = true;
    }
    @Override
    public void write(Batch batch) throws StageException {
    }
  }

  @SuppressWarnings("unchecked")
  /**
   *
   * @return Mock stage library implementation
   */
  public static StageLibraryTask createMockStageLibrary() {
    StageLibraryTask lib = Mockito.mock(StageLibraryTask.class);
    ConfigDefinition configDef1 = new ConfigDefinition("string", ConfigDef.Type.STRING, ConfigDef.Upload.NO, "l1", "d1", "--", true, "g",
        "stringVar", null, "", new ArrayList<>(), 0, Collections.<ElFunctionDefinition>emptyList(),
      Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<Class> emptyList(),
      ConfigDef.Evaluation.IMPLICIT, null, ConfigDef.DisplayMode.BASIC, "");
    ConfigDefinition configDef2 = new ConfigDefinition("int", ConfigDef.Type.NUMBER, ConfigDef.Upload.NO, "l2", "d2", "-1", true, "g", "intVar", null, "",
      new ArrayList<>(), 0, Collections.<ElFunctionDefinition>emptyList(),
      Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<Class> emptyList(),
      ConfigDef.Evaluation.IMPLICIT, null, ConfigDef.DisplayMode.BASIC, "");
    ConfigDefinition configDef3 = new ConfigDefinition("long", ConfigDef.Type.NUMBER, ConfigDef.Upload.NO, "l3", "d3", "-2", true, "g", "longVar", null, "",
      new ArrayList<>(), 0, Collections.<ElFunctionDefinition>emptyList(),
      Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<Class> emptyList(),
      ConfigDef.Evaluation.IMPLICIT, null, ConfigDef.DisplayMode.BASIC, "");
    ConfigDefinition configDef4 = new ConfigDefinition("boolean", ConfigDef.Type.BOOLEAN, ConfigDef.Upload.NO, "l4", "d4", "false", true, "g", "booleanVar",
      null, "", new ArrayList<>(), 0, Collections.<ElFunctionDefinition>emptyList(),
      Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<Class> emptyList(),
      ConfigDef.Evaluation.IMPLICIT, null, ConfigDef.DisplayMode.BASIC, "");
    StageDefinition sourceDef = new StageDefinitionBuilder(TestUtil.class.getClassLoader(), TSource.class, "source")
      .withStageDef(Mockito.mock(StageDef.class))
      .withConfig(configDef1, configDef2, configDef3, configDef4)
      .withExecutionModes(ExecutionMode.CLUSTER_BATCH, ExecutionMode.STANDALONE)
      .build();
    StageDefinition targetDef = new StageDefinitionBuilder(TestUtil.class.getClassLoader(), TTarget.class, "target")
      .withStageDef(Mockito.mock(StageDef.class))
      .withExecutionModes(ExecutionMode.CLUSTER_BATCH, ExecutionMode.STANDALONE)
      .build();

    Mockito.when(lib.getStage(Mockito.eq("library"), Mockito.eq("source"), Mockito.eq(false)))
           .thenReturn(sourceDef);
    Mockito.when(lib.getStage(Mockito.eq("library"), Mockito.eq("target"), Mockito.eq(false)))
           .thenReturn(targetDef);

    List<StageDefinition> stages = new ArrayList<>(2);
    stages.add(sourceDef);
    stages.add(targetDef);
    Mockito.when(lib.getStages()).thenReturn(stages);

    Mockito.when(lib.getPipeline()).thenReturn(PipelineDefinition.getPipelineDef());
    Mockito.when(lib.getPipelineFragment()).thenReturn(PipelineFragmentDefinition.getPipelineFragmentDef());
    Mockito.when(lib.getPipelineRules()).thenReturn(PipelineRulesDefinition.getPipelineRulesDef());
    return lib;
  }

  public static class StageLibraryTestInjector implements Factory<StageLibraryTask> {

    public StageLibraryTestInjector() {
    }

    @Singleton
    @Override
    public StageLibraryTask provide() {
      return createMockStageLibrary();
    }

    @Override
    public void dispose(StageLibraryTask stageLibrary) {
    }
  }

  public static class URITestInjector implements Factory<URI> {
    @Override
    public URI provide() {
      try {
        return new URI("URIInjector");
      } catch (URISyntaxException e) {
        LOG.debug("Ignoring exception", e);
        return null;
      }
    }

    @Override
    public void dispose(URI uri) {
    }
  }

  public static class PrincipalTestInjector implements Factory<Principal> {

    @Override
    public Principal provide() {
      return new Principal() {
        @Override
        public String getName() {
          return "user1";
        }
      };
    }

    @Override
    public void dispose(Principal principal) {
    }
  }

  public static class RuntimeInfoTestInjector implements Factory<RuntimeInfo> {
    @Singleton
    @Override
    public RuntimeInfo provide() {
      RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
      Mockito.when(runtimeInfo.isAclEnabled()).thenReturn(true);
      return runtimeInfo;
    }

    @Override
    public void dispose(RuntimeInfo runtimeInfo) {
    }
  }

  public static class BuildInfoTestInjector implements Factory<BuildInfo> {
    @Singleton
    @Override
    public BuildInfo provide() {
      BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
      Mockito.when(buildInfo.getVersion()).thenReturn("3.17.0");
      return buildInfo;
    }

    @Override
    public void dispose(BuildInfo buildInfo) {
    }
  }

  public static class RuntimeInfoTestInjectorForSlaveMode implements Factory<RuntimeInfo> {
    @Singleton
    @Override
    public RuntimeInfo provide() {
      RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
      return runtimeInfo;
    }

    @Override
    public void dispose(RuntimeInfo runtimeInfo) {
    }
  }

  public static class UserGroupManagerTestInjector implements Factory<UserGroupManager> {
    @Singleton
    @Override
    public UserGroupManager provide() {
      UserGroupManager userGroupManager = Mockito.mock(UserGroupManager.class);
      UserJson userJson = new UserJson();
      userJson.setName("user1");
      userJson.setGroups(Collections.<String>emptyList());
      Mockito.when(userGroupManager.getUser((Principal) Mockito.anyObject()))
          .thenReturn(userJson);
      return userGroupManager;
    }

    @Override
    public void dispose(UserGroupManager userGroupManager) {
    }
  }

  public static class CredentialStoreTaskTestInjector implements Factory<CredentialStoresTask> {

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

    private static class MyManagedCredentialStore implements ManagedCredentialStore {
      Map<String, String> credentialStoreMap = new HashMap<>();

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

    public static final ManagedCredentialStore INSTANCE = new MyManagedCredentialStore();

    @Singleton
    @Override
    public CredentialStoresTask provide() {
      CredentialStoresTask credentialStoresTask = Mockito.mock(CredentialStoresTask.class);

      CredentialStoreDefinition credentialStoreDefinition = Mockito.mock(CredentialStoreDefinition.class);
      StageLibraryDefinition stageLibraryDefinition = Mockito.mock(StageLibraryDefinition.class);
      Mockito.when(stageLibraryDefinition.getName()).thenReturn("streamsets");
      Mockito.when(stageLibraryDefinition.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());

      Mockito.when(credentialStoreDefinition.getName()).thenReturn("streamsets");
      Mockito.when(credentialStoreDefinition.getStageLibraryDefinition()).thenReturn(stageLibraryDefinition);
      Mockito.doReturn(MyManagedCredentialStore.class).when(credentialStoreDefinition).getStoreClass();
      Mockito.when(credentialStoresTask.getConfiguredStoreDefinititions()).thenReturn(ImmutableList.of(credentialStoreDefinition));
      Mockito.when(credentialStoresTask.getDefaultManagedCredentialStore()).thenReturn(INSTANCE);

      return credentialStoresTask;
    }

    @Override
    public void dispose(CredentialStoresTask task) {

    }
  }


}
