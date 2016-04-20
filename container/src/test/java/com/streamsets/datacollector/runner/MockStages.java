/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.runner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.streamsets.datacollector.cluster.ClusterModeConstants;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.DeliveryGuarantee;
import com.streamsets.datacollector.config.ModelDefinition;
import com.streamsets.datacollector.config.ModelType;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineDefinition;
import com.streamsets.datacollector.config.RawSourceDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.el.ElConstantDefinition;
import com.streamsets.datacollector.el.ElFunctionDefinition;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.OffsetCommitTrigger;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.RawSourcePreviewer;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.impl.ClusterSource;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class MockStages {

  @SuppressWarnings("unchecked")
  public static StageConfiguration createSource(String instanceName, List<String> outputs) {

    return new StageConfiguration(instanceName, "default", "sourceName", 1,
      Collections.<Config>emptyList(), null, Collections.<String>emptyList(),
      outputs);
  }

  @SuppressWarnings("unchecked")
  public static StageConfiguration createProcessor(String instanceName, List<String> inputs, List<String> outputs) {
    return new StageConfiguration(
      instanceName, "default", "processorName", 1,
      Collections.<Config>emptyList(), null, inputs, outputs);
  }

  @SuppressWarnings("unchecked")
  public static StageConfiguration createTarget(String instanceName, List<String> inputs) {
    return new StageConfiguration(
      instanceName, "default", "targetName", 1,
      Collections.<Config>emptyList(), null, inputs, Collections.<String>emptyList());
  }

  private static Source sourceCapture;
  private static Processor processorCapture;
  private static Target targetCapture;
  private static Target errorCapture;

  // it must be called after the pipeline is built
  public static void setSourceCapture(Source s) {
    sourceCapture = s;
  }

  // it must be called after the pipeline is built
  public static void setProcessorCapture(Processor p) {
    processorCapture = p;
  }

  // it must be called after the pipeline is built
  public static void setTargetCapture(Target t) {
    targetCapture = t;
  }

  // it must be called after the pipeline is built
  public static void setErrorStageCapture(Target t) {
    errorCapture = t;
  }


  public static class MockRawSourcePreviewer implements RawSourcePreviewer {

    @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "localhost",
      label = "Broker Host",
      description = "",
      displayPosition = 10
    )
    public String brokerHost;

    @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "9092",
      label = "Broker Port",
      description = "",
      displayPosition = 20,
      min = 1,
      max = Integer.MAX_VALUE
    )
    public int brokerPort;

    @Override
    public InputStream preview(int maxLength) {
      StringBuilder sb = new StringBuilder();
      sb.append(brokerHost).append(":").append(brokerPort);
      return new ByteArrayInputStream(sb.toString().getBytes());
    }

    @Override
    public String getMimeType() {
      return "*/*";
    }

    @Override
    public void setMimeType(String mimeType) {

    }
  }

  @RawSource(rawSourcePreviewer = MockRawSourcePreviewer.class, mimeType = "*/*")
  public static class MSource implements Source, ErrorListener {

    @Override
    public List<ConfigIssue> init(Info info, Context context) {
      if (sourceCapture != null) {
        return sourceCapture.init(info, context);
      } else {
        return Collections.emptyList();
      }
    }

    @Override
    public void destroy() {
      if (sourceCapture != null) {
        sourceCapture.destroy();
      }
    }

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      if (sourceCapture != null) {
        return sourceCapture.produce(lastSourceOffset, -1, batchMaker);
      }
      return null;
    }

    @Override
    public void errorNotification(Throwable throwable) {
      if (sourceCapture != null && sourceCapture instanceof ErrorListener) {
        ((ErrorListener)sourceCapture).errorNotification(throwable);
      }
    }
  }

  public static class ComplexSource implements Source {

    @Override
    public List<ConfigIssue> init(Info info, Context context) {
      if (sourceCapture != null) {
        return sourceCapture.init(info, context);
      } else {
        return Collections.emptyList();
      }
    }

    @Override
    public void destroy() {
      if (sourceCapture != null) {
        sourceCapture.destroy();
      }
    }

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      if (sourceCapture != null) {
        return sourceCapture.produce(lastSourceOffset, -1, batchMaker);
      }
      return null;
    }
  }

  public static class ClusterMSource implements ClusterSource {

    public static boolean MOCK_VALIDATION_ISSUES = false;

    @Override
    public List<ConfigIssue> init(Info info, Context context) {
      if (MOCK_VALIDATION_ISSUES) {
        List<ConfigIssue> issues = new ArrayList<ConfigIssue>();
        issues.add(context.createConfigIssue("a", "b", ContainerError.CONTAINER_0001, "dummy_stage_error"));
        return issues;
      } else if (sourceCapture != null) {
        return sourceCapture.init(info, context);
      } else {
        return Collections.emptyList();
      }
    }

    @Override
    public void destroy() {
      if (sourceCapture != null) {
        sourceCapture.destroy();
      }
    }

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      if (sourceCapture != null) {
        return sourceCapture.produce(lastSourceOffset, -1, batchMaker);
      }
      return null;
    }

    @Override
    public int getParallelism() {
      return 25;
    }

    @Override
    public void put(List<Map.Entry> batch) throws InterruptedException {

    }

    @Override
    public long getRecordsProduced() {
      return 0;
    }

    @Override
    public boolean inErrorState() {
      return false;
    }

    @Override
    public String getName() {
      return "ClusterMSource";
    }

    @Override
    public boolean isInBatchMode() {
      return false;
    }

    @Override
    public Map<String, String> getConfigsToShip() {
      return new HashMap<String, String>();
    }

    @Override
    public void shutdown() {

    }

    @Override
    public void postDestroy() {
    }

  }

  public static class MSourceOffsetCommitter extends MSource implements OffsetCommitter {

    @Override
    public void commit(String offset) throws StageException {
      if (sourceCapture != null && sourceCapture instanceof OffsetCommitter) {
        ((OffsetCommitter) sourceCapture).commit(offset);
      }
    }
  }

  public static class MProcessor implements Processor {

    @Override
    public List<ConfigIssue> init(Info info, Context context) {
      if (processorCapture != null) {
        return processorCapture.init(info, context);
      } else {
        return Collections.emptyList();
      }
    }

    @Override
    public void destroy() {
      if (processorCapture != null) {
        processorCapture.destroy();
      }
    }

    @Override
    public void process(Batch batch, BatchMaker batchMaker) throws StageException {
      if (processorCapture != null) {
        processorCapture.process(batch, batchMaker);
      }
    }
  }

  public static class MTarget implements Target {

    @Override
    public List<ConfigIssue> init(Info info, Context context) {
      if (targetCapture != null) {
        return targetCapture.init(info, context);
      } else {
        return Collections.emptyList();
      }
    }

    @Override
    public void destroy() {
      if (targetCapture != null) {
        targetCapture.destroy();
      }
    }

    @Override
    public void write(Batch batch) throws StageException {
      if (targetCapture != null) {
        targetCapture.write(batch);
      }
    }
  }

  public static class OffsetControllerTarget extends BaseTarget implements OffsetCommitTrigger {

    private boolean commit = false;

    @Override
    public List<Stage.ConfigIssue> init(Info info, Target.Context context) {
      if (targetCapture != null) {
        return targetCapture.init(info, context);
      } else {
        return Collections.emptyList();
      }
    }

    @Override
    public void destroy() {
      if (targetCapture != null) {
        targetCapture.destroy();
      }
    }

    @Override
    public void write(Batch batch) throws StageException {
      commit = false;
      if (targetCapture != null) {
        targetCapture.write(batch);
      }
      commit = true;
    }

    @Override
    public boolean commit() {
      return commit;
    }
  }

  public static class ETarget implements Target {

    //This field is required, even though its not used, to pass validation as it tries to inject value into a field
    //with this name
    public String errorTargetConfFieldName;

    @Override
    public List<ConfigIssue> init(Info info, Context context) {
      if (errorCapture != null) {
        return errorCapture.init(info, context);
      } else {
        return Collections.emptyList();
      }
    }

    @Override
    public void destroy() {
      if (errorCapture != null) {
        errorCapture.destroy();
      }
    }

    @Override
    public void write(Batch batch) throws StageException {
      if (errorCapture != null) {
        errorCapture.write(batch);
      }
    }
  }

  public static class StatsTarget implements Target {

    @Override
    public List<ConfigIssue> init(Info info, Context context) {
      if (errorCapture != null) {
        return errorCapture.init(info, context);
      } else {
        return Collections.emptyList();
      }
    }

    @Override
    public void destroy() {
      if (errorCapture != null) {
        errorCapture.destroy();
      }
    }

    @Override
    public void write(Batch batch) throws StageException {
      if (errorCapture != null) {
        errorCapture.write(batch);
      }
    }
  }

  public static StageLibraryTask createStageLibrary(ClassLoader cl) {
    return new MockStageLibraryTask.Builder(cl).build();
  }

  public static StageLibraryTask createClusterStreamingStageLibrary(ClassLoader cl) {
    return new MockStageLibraryTask.ClusterStreamingBuilder(cl).build();
  }

  public static StageLibraryTask createClusterBatchStageLibrary(ClassLoader cl) {
    return new MockStageLibraryTask.ClusterBatchBuilder(cl).build();
  }


  public static StageLibraryTask createStageLibrary() {
    return createStageLibrary(Thread.currentThread().getContextClassLoader());
  }

  private static final StageLibraryDefinition createLibraryDef(ClassLoader cl) {
    return new StageLibraryDefinition(cl, "default", "", new Properties(), null, null, null) {
      @Override
      public List<ExecutionMode> getStageExecutionModesOverride(Class klass) {
        return ImmutableList.copyOf(ExecutionMode.values());
      }
    };
  }

  public static class MockStageLibraryTask implements StageLibraryTask {
    private final List<StageDefinition> stages;

    private MockStageLibraryTask(Collection<StageDefinition> stages) {
      this.stages = ImmutableList.copyOf(stages);
    }
    @Override
    public String getName() {
      return null;
    }

    @Override
    public void init() {

    }

    @Override
    public void run() {

    }

    @Override
    public void waitWhileRunning() throws InterruptedException {

    }

    @Override
    public void stop() {

    }

    @Override
    public Status getStatus() {
      return null;
    }

    @Override
    public PipelineDefinition getPipeline() {
      return PipelineDefinition.getPipelineDef();
    }

    @Override
    public List<StageDefinition> getStages() {
      return stages;
    }

    @Override
    public StageDefinition getStage(String library, String name, boolean forExecution) {
      for (StageDefinition def : stages) {
        if (def.getLibrary().equals(library) && def.getName().equals(name)) {
          return def;
        }
      }
      return null;
    }

    @Override
    public Map<String, String> getLibraryNameAliases() {
      return Collections.emptyMap();
    }

    @Override
    public Map<String, String> getStageNameAliases() {
      return Collections.emptyMap();
    }

    @Override
    public void releaseStageClassLoader(ClassLoader classLoader) {
    }

    public static class Builder {
      private final Map<String, StageDefinition> stages;

      public Builder() {
        this(Thread.currentThread().getContextClassLoader());
      }

      public Builder(ClassLoader cl) {

        ConfigDefinition brokerHostConfig = new ConfigDefinition("brokerHost", ConfigDef.Type.STRING, "brokerHost", "",
          "", true, "", "brokerHost", null, "", null, 10, Collections.<ElFunctionDefinition>emptyList(),
          Collections.<ElConstantDefinition>emptyList(), 0, 0,
          "", 0, Collections.<Class>emptyList(), ConfigDef.Evaluation.IMPLICIT, Collections.<String, List<Object>>emptyMap());
        ConfigDefinition brokerPortConfig = new ConfigDefinition("brokerPort", ConfigDef.Type.NUMBER, "brokerPort", "",
          "", true, "", "brokerPort", null, "", null, 10, Collections.<ElFunctionDefinition>emptyList(),
          Collections.<ElConstantDefinition>emptyList(), 0, 0,
          "", 0, Collections.<Class>emptyList(), ConfigDef.Evaluation.IMPLICIT, Collections.<String, List<Object>>emptyMap());

        RawSourceDefinition rawSourceDefinition = new RawSourceDefinition(MockRawSourcePreviewer.class.getName(), "*/*",
          Arrays.asList(brokerHostConfig, brokerPortConfig));

        StageDefinition sDef = new StageDefinition(createLibraryDef(cl),
                                                   false, MSource.class, "sourceName", 1, "sourceLabel",
          "sourceDesc", StageType.SOURCE, false,  true, true, Collections.<ConfigDefinition>emptyList(),
          rawSourceDefinition, "", null, false, 1, null,
          Arrays.asList(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH), false,
          new StageUpgrader.Default(), Collections.<String>emptyList(), false, "", false, false);

        StageDefinition socDef = new StageDefinition(createLibraryDef(cl),
                                                     false, MSourceOffsetCommitter.class, "sourceOffsetCommitterName", 1, "sourceOffsetCommitterLabel",
          "sourceDesc", StageType.SOURCE, false, true, true, Collections.<ConfigDefinition>emptyList(),
          null/*raw source definition*/, "", null, false, 1, null,
          Arrays.asList(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH), false,
          new StageUpgrader.Default(), Collections.<String>emptyList(), false,
            "", false, false);

        StageDefinition pDef = new StageDefinition(createLibraryDef(cl),
                                                   false, MProcessor.class, "processorName", 1, "sourcelabel",
          "sourceDescription", StageType.PROCESSOR, false, true, true, Collections.<ConfigDefinition>emptyList(),
          null/*raw source definition*/, "", null,
          false, 1, null,
          Arrays.asList(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH), false,
          new StageUpgrader.Default(), Collections.<String>emptyList(), false, "", false, false);

        ModelDefinition m = new ModelDefinition(ModelType.FIELD_SELECTOR_MULTI_VALUE, null, Collections.<String>emptyList(),
          Collections.<String>emptyList(), null, null);
        ConfigDefinition stageReqField = new ConfigDefinition("stageRequiredFields", ConfigDef.Type.MODEL, "stageRequiredFields",
          "stageRequiredFields", null, false, "groupName", "stageRequiredFieldName", m, "", null, 0, Collections.<ElFunctionDefinition>emptyList(),
          Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<Class> emptyList(),
          ConfigDef.Evaluation.IMPLICIT, null);

        StageDefinition tDef = new StageDefinition(createLibraryDef(cl),
                                                   false, MTarget.class, "targetName", 1, "targetLabel",
          "targetDesc", StageType.TARGET, false, true, true, Arrays.asList(stageReqField),
          null/*raw source definition*/, "", null, false, 0, null,
          Arrays.asList(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH,
            ExecutionMode.CLUSTER_MESOS_STREAMING), false,
          new StageUpgrader.Default(), Collections.<String>emptyList(), false, "", false, false);

        ConfigDefinition reqField = new ConfigDefinition(
          "requiredFieldConfName", ConfigDef.Type.STRING, "requiredFieldLabel", "requiredFieldDesc", 10, true,
          "groupName", "requiredFieldFieldName", null, "", null, 0, Collections.<ElFunctionDefinition>emptyList(),
          Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<Class> emptyList(),
          ConfigDef.Evaluation.IMPLICIT, null);

        StageDefinition targetWithReqField = new StageDefinition(createLibraryDef(cl),
          false, MTarget.class, "targetWithReqField", 1, "targetWithReqField",
          "targetWithReqField", StageType.TARGET, false, true, true, Arrays.asList(reqField),
          null/*raw source definition*/, "", null, false, 0, null,
          Arrays.asList(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH), false,
          new StageUpgrader.Default(), Collections.<String>emptyList(), false, "", false, false);

        //error target configurations
        ConfigDefinition errorTargetConf = new ConfigDefinition(
          "errorTargetConfName", ConfigDef.Type.STRING, "errorTargetConfLabel", "errorTargetConfDesc",
          "/SDC_HOME/errorDir", true, "groupName", "errorTargetConfFieldName", null, "", null , 0,
          Collections.<ElFunctionDefinition>emptyList(), Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0,
          Collections.<Class> emptyList(), ConfigDef.Evaluation.IMPLICIT, null);

        StageDefinition eDef = new StageDefinition(createLibraryDef(cl),
                                                   false, ETarget.class, "errorTarget", 1, "errorTarget",
          "Error Target", StageType.TARGET, true, false, true,
          Arrays.asList(errorTargetConf), null/*raw source definition*/, "", null, false, 0, null,
          Arrays.asList(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH,
            ExecutionMode.CLUSTER_MESOS_STREAMING), false,
          new StageUpgrader.Default(), Collections.<String>emptyList(), false, "", false, false);

        StageDefinition statsDef = new StageDefinition(createLibraryDef(cl),
          false, StatsTarget.class, "statsAggregator", 1, "statsAggregator",
          "Stats Aggregator", StageType.TARGET, false, false, true,
          Collections.<ConfigDefinition>emptyList(), null/*raw source definition*/, "", null, false, 0, null,
          Arrays.asList(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH,
            ExecutionMode.CLUSTER_MESOS_STREAMING), false,
          new StageUpgrader.Default(), Collections.<String>emptyList(), false, "", true, false);

        ConfigDefinition depConfDef = new ConfigDefinition(
          "dependencyConfName", ConfigDef.Type.NUMBER, "dependencyConfLabel", "dependencyConfDesc", 5, true,
          "groupName", "dependencyConfFieldName", null, "", null, 0, Collections.<ElFunctionDefinition>emptyList(),
          Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<Class> emptyList(),
          ConfigDef.Evaluation.IMPLICIT, null);
        List<Object> triggeredBy = new ArrayList<>();
        triggeredBy.add(1);
        ConfigDefinition triggeredConfDef = new ConfigDefinition(
          "triggeredConfName", ConfigDef.Type.NUMBER, "triggeredConfLabel", "triggeredConfDesc", 10, true,
          "groupName", "triggeredConfFieldName", null, "dependencyConfName", triggeredBy, 0,
          Collections.<ElFunctionDefinition>emptyList(), Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0,
          Collections.<Class> emptyList(), ConfigDef.Evaluation.IMPLICIT, null);
        StageDefinition swcDef = new StageDefinition(createLibraryDef(cl),
                                                     false, MSource.class, "sourceWithConfigsName", 1, "sourceWithConfigsLabel",
          "sourceWithConfigsDesc", StageType.SOURCE, false, true, true,
          Lists.newArrayList(depConfDef, triggeredConfDef), null/*raw source definition*/, "", null, false, 1, null,
          Arrays.asList(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH), false,
          new StageUpgrader.Default(), Collections.<String>emptyList(), false, "", false, false);

        StageDefinition clusterStageDef = new StageDefinition(createLibraryDef(cl),
                                                              false, ClusterMSource.class, "clusterSource", 1, "clusterSourceLabel",
            "clusterSourceDesc", StageType.SOURCE, false, true, true,
            Collections.<ConfigDefinition>emptyList(), null, "", null, false, 1, null,
            Arrays.asList(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.CLUSTER_BATCH,
              ExecutionMode.CLUSTER_MESOS_STREAMING), false, new StageUpgrader.Default(),
            Collections.<String> emptyList(), false, "", false, false);

        StageDefinition clusterLibraryStageDef = new StageDefinition(createLibraryDef(cl),
                                                                     false, ClusterMSource.class, "clusterLibrarySource", 1, "clusterSourceLabel",
          "clusterSourceDesc", StageType.SOURCE, false, true, true,
          Collections.<ConfigDefinition>emptyList(), null, "", null, false, 1, null,
          Arrays.asList(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.CLUSTER_BATCH), false,
          new StageUpgrader.Default(), Collections.<String>emptyList(), false, "", false, false);

        StageDefinition commonLibraryTargetDef = new StageDefinition(createLibraryDef(cl),
                                                                     false, MTarget.class, "commonLibraryTarget", 1, "commonLibraryTargetLabel",
          "commonLibraryTargetDesc", StageType.TARGET, false, true, true,
          Collections.<ConfigDefinition>emptyList(), null, "", null, false, 1, null,
          Arrays.asList(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH), false,
          new StageUpgrader.Default(), Collections.<String>emptyList(), false, "", false, false);

        ConfigDefinition regularConf = new ConfigDefinition(
          "regularConfName", ConfigDef.Type.NUMBER, "regularConfLabel", "regularConfDesc", 10, true,
          "groupName", "regularConfFieldName", null, "", null, 0, Collections.<ElFunctionDefinition>emptyList(),
          Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<Class> emptyList(),
          ConfigDef.Evaluation.IMPLICIT, null);

        List<ConfigDefinition> list = new ArrayList<>();
        list.add(regularConf);
        ModelDefinition modelDefinition = new ModelDefinition(ModelType.LIST_BEAN, null, Collections.<String>emptyList(),
          Collections.<String>emptyList(), null, list);

        ConfigDefinition complexConf = new ConfigDefinition(
          "complexConfName", ConfigDef.Type.MODEL, "complexConfLabel", "complexConfDesc", null, true,
          "groupName", "complexConfFieldName", modelDefinition, "", null, 0, Collections.<ElFunctionDefinition>emptyList(),
          Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<Class> emptyList(),
          ConfigDef.Evaluation.IMPLICIT, null);

        StageDefinition complexStage = new StageDefinition(createLibraryDef(cl),
                                                           false, ComplexSource.class, "complexStageName", 1, "complexStageLabel",
          "complexStageDesc", StageType.SOURCE, false, true, true,
          Lists.newArrayList(complexConf), null/*raw source definition*/, "", null, false, 1, null,
          Arrays.asList(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH), false,
          new StageUpgrader.Default(), Collections.<String>emptyList(), false, "", false, false);

        StageDefinition offsetControlTarget = new StageDefinition(createLibraryDef(cl),
          false, OffsetControllerTarget.class, "offsetControlTarget", 1, "tLabel",
          "tDesc", StageType.TARGET, false, true, true,
          Collections.<ConfigDefinition>emptyList(), null/*raw source definition*/, "", null, false, 1, null,
          Arrays.asList(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH), false,
          new StageUpgrader.Default(), Collections.<String>emptyList(), false, "", false, true);

        StageDefinition multiLaneSource = new StageDefinition(createLibraryDef(cl),
          false, OffsetControllerTarget.class, "multiLaneSource", 1, "multiLaneSourceLabel",
          "multiLaneSourceDesc", StageType.SOURCE, false, true, true,
          Collections.<ConfigDefinition>emptyList(), null/*raw source definition*/, "", null, false, 2, null,
          Arrays.asList(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH), false,
          new StageUpgrader.Default(), Collections.<String>emptyList(), false, "", false, false);

        StageDefinition[] stageDefs =
          new StageDefinition[] {
              sDef,
              socDef,
              pDef,
              tDef,
              targetWithReqField,
              swcDef,
              eDef,
              statsDef,
              clusterStageDef,
              complexStage,
              clusterLibraryStageDef,
              commonLibraryTargetDef,
              offsetControlTarget,
              multiLaneSource
          };
        stages = new HashMap<>();
        for (StageDefinition def : stageDefs) {
          if (stages.containsKey(def.getName())) {
            throw new IllegalStateException("Duplicate stage at " + def.getName());
          }
          stages.put(def.getName(), def);
        }
      }

      public Builder override(StageDefinition def) {
        if (stages.containsKey(def.getName())) {
          stages.put(def.getName(), def);
        } else {
          throw new IllegalStateException("Expected stage at " + def.getName());
        }
        return this;
      }
      public Builder overrideClass(String name, Class klass) {
        if (stages.containsKey(name)) {
          StageDefinition oldDef = stages.get(name);
          StageDefinition newDef = new StageDefinition(createLibraryDef(klass.getClassLoader()),
                                                       false, klass, oldDef.getName(), oldDef.getVersion(), oldDef.getLabel(),
            oldDef.getDescription(), oldDef.getType(), oldDef.isErrorStage(), oldDef.hasPreconditions(),
            oldDef.hasOnRecordError(), oldDef.getConfigDefinitions(),
            oldDef.getRawSourceDefinition(), oldDef.getIcon(), oldDef.getConfigGroupDefinition(),
            oldDef.isVariableOutputStreams(), oldDef.getOutputStreams(), oldDef.getOutputStreamLabelProviderClass(),
            Arrays.asList(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE), false, new StageUpgrader.Default(),
            Collections.<String>emptyList(), false, "", false, false);
          stages.put(name, newDef);
        } else {
          throw new IllegalStateException("Expected stage at " + name);
        }
        return this;
      }

      public StageLibraryTask build() {
        return new MockStageLibraryTask(stages.values());
      }
    }

    public static RawSourceDefinition getRawSourceDefinition() {
      ConfigDefinition brokerHostConfig = new ConfigDefinition("brokerHost", ConfigDef.Type.STRING, "brokerHost", "",
        "", true, "", "brokerHost", null, "", null, 10, Collections.<ElFunctionDefinition>emptyList(),
        Collections.<ElConstantDefinition>emptyList(), 0, 0,
        "", 0, Collections.<Class>emptyList(), ConfigDef.Evaluation.IMPLICIT, Collections.<String, List<Object>>emptyMap());
      ConfigDefinition brokerPortConfig = new ConfigDefinition("brokerPort", ConfigDef.Type.NUMBER, "brokerPort", "",
        "", true, "", "brokerPort", null, "", null, 10, Collections.<ElFunctionDefinition>emptyList(),
        Collections.<ElConstantDefinition>emptyList(), 0, 0,
        "", 0, Collections.<Class>emptyList(), ConfigDef.Evaluation.IMPLICIT, Collections.<String, List<Object>>emptyMap());

      RawSourceDefinition rawSourceDefinition = new RawSourceDefinition(MockRawSourcePreviewer.class.getName(), "*/*",
        Arrays.asList(brokerHostConfig, brokerPortConfig));
      return rawSourceDefinition;
    }

    public static StageDefinition getErrorStageDefinition(ClassLoader cl) {
     //error target configurations
      ConfigDefinition errorTargetConf = new ConfigDefinition(
        "errorTargetConfName", ConfigDef.Type.STRING, "errorTargetConfLabel", "errorTargetConfDesc",
        "/SDC_HOME/errorDir", true, "groupName", "errorTargetConfFieldName", null, "", null , 0,
        Collections.<ElFunctionDefinition>emptyList(), Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0,
        Collections.<Class> emptyList(), ConfigDef.Evaluation.IMPLICIT, null);

      StageDefinition errorTargetStageDef = new StageDefinition(createLibraryDef(cl),
                                                 false, ETarget.class, "errorTarget", 1, "errorTarget",
        "Error Target", StageType.TARGET, true, false, true,
        Arrays.asList(errorTargetConf), null/*raw source definition*/, "", null, false, 0, null,
        Arrays.asList(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.CLUSTER_BATCH, ExecutionMode.STANDALONE), false,
        new StageUpgrader.Default(), Collections.<String>emptyList(), false, "", false, false);
      return errorTargetStageDef;
    }

    public static StageDefinition getStatsAggStageDefinition(ClassLoader cl) {

      StageDefinition errorTargetStageDef = new StageDefinition(createLibraryDef(cl),
        false, StatsTarget.class, "statsAggregator", 1, "statsAggregator",
        "Stats Aggregator", StageType.TARGET, false, false, true,
        Collections.<ConfigDefinition>emptyList(), null/*raw source definition*/, "", null, false, 0, null,
        Arrays.asList(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.CLUSTER_BATCH, ExecutionMode.STANDALONE), false,
        new StageUpgrader.Default(), Collections.<String>emptyList(), false, "", true, false);
      return errorTargetStageDef;
    }

    public static class ClusterStreamingBuilder {
      private final StageDefinition clusterStageDef;
      private final StageDefinition errorTargetStageDef;
      private final StageDefinition statsTargetStageDef;

      public ClusterStreamingBuilder() {
        this(Thread.currentThread().getContextClassLoader());
      }

      public ClusterStreamingBuilder(ClassLoader cl) {
        clusterStageDef =
          new StageDefinition(createLibraryDef(cl), false, MSource.class, "sourceName", 1, "sourceLabel", "sourceDesc",
            StageType.SOURCE, false, true, true, Collections.<ConfigDefinition> emptyList(), getRawSourceDefinition(),
            "", null, false, 1, null, Arrays.asList(ExecutionMode.CLUSTER_BATCH, ExecutionMode.CLUSTER_YARN_STREAMING,
              ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_MESOS_STREAMING), false, new StageUpgrader.Default(),
            Arrays.asList(ClusterModeConstants.SPARK_KAFKA_JAR_REGEX), false, "", false, false);
        errorTargetStageDef = getErrorStageDefinition(cl);
        statsTargetStageDef = getStatsAggStageDefinition(cl);
      }


      public StageLibraryTask build() {
        return new MockStageLibraryTask(ImmutableList.of(clusterStageDef, errorTargetStageDef, statsTargetStageDef));
      }
    }

    public static class ClusterBatchBuilder {
      private final StageDefinition clusterStageDef;
      private final StageDefinition errorTargetStageDef;
      private final StageDefinition statsTargetStageDef;

      public ClusterBatchBuilder() {
        this(Thread.currentThread().getContextClassLoader());
      }

      public ClusterBatchBuilder(ClassLoader cl) {
        clusterStageDef = new StageDefinition(
            createLibraryDef(cl),
            false,
            MSource.class,
            "sourceName",
            1,
            "sourceLabel",
            "sourceDesc",
            StageType.SOURCE,
            false,
            true,
            true,
            Collections.<ConfigDefinition> emptyList(),
            getRawSourceDefinition(),
            "",
            null,
            false,
            1,
            null,
            Arrays.asList(ExecutionMode.CLUSTER_BATCH, ExecutionMode.STANDALONE),
            false,
            new StageUpgrader.Default(),
            Arrays.asList(ClusterModeConstants.AVRO_JAR_REGEX, ClusterModeConstants.AVRO_MAPRED_JAR_REGEX),
            false,
            "",
            false,
            false
        );
        errorTargetStageDef = getErrorStageDefinition(cl);
        statsTargetStageDef = getStatsAggStageDefinition(cl);
      }

      public StageLibraryTask build() {
        return new MockStageLibraryTask(ImmutableList.of(clusterStageDef, errorTargetStageDef, statsTargetStageDef));
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static List<StageConfiguration> getSourceStageConfig() {
    StageConfiguration source = new StageConfiguration("s", "default", "sourceName", 1,
      Collections.<Config>emptyList(), null, Collections.<String>emptyList(), ImmutableList.of("s"));
    List<StageConfiguration> stages = new ArrayList<>();
    stages.add(source);
    return stages;
  }

  public static void resetStageCaptures() {
    sourceCapture = null;
    processorCapture = null;
    targetCapture = null;
  }

  public static PipelineConfiguration createPipelineConfigurationSourceProcessorTarget() {
    return createPipelineConfigurationSourceProcessorTarget(PipelineStoreTask.SCHEMA_VERSION);
  }

  public static PipelineConfiguration createPipelineConfigurationSourceProcessorTargetHigherVersion() {
    return createPipelineConfigurationSourceProcessorTarget(PipelineStoreTask.SCHEMA_VERSION + 1);
  }

  public static PipelineConfiguration createPipelineConfigurationComplexSourceProcessorTarget() {
    return createPipelineConfigurationComplexSourceProcessorTarget(PipelineStoreTask.SCHEMA_VERSION);
  }

  @SuppressWarnings("unchecked")
  public static StageConfiguration getErrorStageConfig() {
    return new StageConfiguration("errorStage", "default", "errorTarget", 1,
      Arrays.asList(new Config("errorTargetConfName", "/SDC_HOME/errorDir")), null, Collections.<String>emptyList(),
      Collections.<String>emptyList());
  }

  public static StageConfiguration getStatsAggregatorStageConfig() {
    return new StageConfiguration(
        "statsAggregator",
        "default",
        "statsAggregator",
        1,
        Collections.<Config>emptyList(), 
        null,
        Collections.<String>emptyList(),
        Collections.<String>emptyList()
    );
  }

  private static List<Config> createPipelineConfigs() {
    List<Config> pipelineConfig = new ArrayList<>();
    pipelineConfig.add(new Config("executionMode", ExecutionMode.STANDALONE.name()));
    return pipelineConfig;
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationSourceProcessorTarget(int schemaVersion) {
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfiguration("s", "default", "sourceName", 1,
      Collections.<Config>emptyList(), null, Collections.<String>emptyList(), ImmutableList.of("s"));
    stages.add(source);
    StageConfiguration processor = new StageConfiguration("p", "default", "processorName", 1,
      Collections.<Config>emptyList(), null, ImmutableList.of("s"), ImmutableList.of("p"));
    stages.add(processor);
    StageConfiguration target = new StageConfiguration("t", "default", "targetName", 1,
      Collections.<Config>emptyList(), null, ImmutableList.of("p"), Collections.<String>emptyList());
    stages.add(target);

    PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(schemaVersion,
        PipelineConfigBean.VERSION,
        UUID.randomUUID(),
        null,
        createPipelineConfigs(),
        null,
        stages,
        getErrorStageConfig(),
        getStatsAggregatorStageConfig()
    );
    pipelineConfiguration.setMetadata(ImmutableMap.of("a", "A"));
    return pipelineConfiguration;
  }


  @SuppressWarnings("unchecked")
  /**
   *     p1 -  p4
   *  s  p2 -  p5  t
   *     p3 -` p6
   */
  public static PipelineConfiguration createPipelineConfigurationComplexSourceProcessorTarget(int schemaVersion) {
    List<StageConfiguration> stages = new ArrayList<>();

    StageConfiguration source = new StageConfiguration("s", "default", "sourceName", 1,
      Collections.<Config>emptyList(), null, Collections.<String>emptyList(), ImmutableList.of("s"));
    stages.add(source);

    StageConfiguration processor1 = new StageConfiguration("p1", "default", "processorName", 1,
      Collections.<Config>emptyList(), null, ImmutableList.of("s"), ImmutableList.of("p1"));
    stages.add(processor1);

    StageConfiguration processor2 = new StageConfiguration("p2", "default", "processorName", 1,
      Collections.<Config>emptyList(), null, ImmutableList.of("s"), ImmutableList.of("p2"));
    stages.add(processor2);

    StageConfiguration processor3 = new StageConfiguration("p3", "default", "processorName", 1,
      Collections.<Config>emptyList(), null, ImmutableList.of("s"), ImmutableList.of("p3"));
    stages.add(processor3);


    StageConfiguration processor4 = new StageConfiguration("p4", "default", "processorName", 1,
      Collections.<Config>emptyList(), null, ImmutableList.of("p1"), ImmutableList.of("p4"));
    stages.add(processor4);

    StageConfiguration processor5 = new StageConfiguration("p5", "default", "processorName", 1,
      Collections.<Config>emptyList(), null, ImmutableList.of("p2"), ImmutableList.of("p5"));
    stages.add(processor5);

    StageConfiguration processor6 = new StageConfiguration("p6", "default", "processorName", 1,
      Collections.<Config>emptyList(), null, ImmutableList.of("p2", "p3"), ImmutableList.of("p6"));
    stages.add(processor6);


    StageConfiguration target = new StageConfiguration("t", "default", "targetName", 1,
      Collections.<Config>emptyList(), null, ImmutableList.of("p4", "p5", "p6"), Collections.<String>emptyList());
    stages.add(target);

    return new PipelineConfiguration(schemaVersion, PipelineConfigBean.VERSION, UUID.randomUUID(), null,
                                     createPipelineConfigs(), null, stages, getErrorStageConfig(), getStatsAggregatorStageConfig());
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationSourceOffsetCommitterProcessorTarget() {
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfiguration("s", "default", "sourceOffsetCommitterName", 1,
      Collections.<Config>emptyList(), null, Collections.<String>emptyList(), ImmutableList.of("s"));
    stages.add(source);
    StageConfiguration processor = new StageConfiguration("p", "default", "processorName", 1,
      Collections.<Config>emptyList(), null, ImmutableList.of("s"), ImmutableList.of("p"));
    stages.add(processor);
    StageConfiguration target = new StageConfiguration("t", "default", "targetName", 1,
      Collections.<Config>emptyList(), null, ImmutableList.of("p"), Collections.<String>emptyList());
    stages.add(target);
    return new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, PipelineConfigBean.VERSION, UUID.randomUUID(),
                                     null, createPipelineConfigs(), null, stages, getErrorStageConfig(), getStatsAggregatorStageConfig());
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineWithRequiredDependentConfig() {
    List<String> lanes = ImmutableList.of("a");
    List<StageConfiguration> stages = new ArrayList<>();

    StageConfiguration source = new StageConfiguration("s", "default", "sourceWithConfigsName", 1,
      Collections.<Config>emptyList(), null, Collections.<String>emptyList(), lanes);
    stages.add(source);
    StageConfiguration target = new StageConfiguration("t", "default", "targetName", 1,
      Collections.<Config>emptyList(), null, lanes, Collections.<String>emptyList());
    stages.add(target);
    return new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, PipelineConfigBean.VERSION, UUID.randomUUID(),
                                     null, createPipelineConfigs(), null, stages, getErrorStageConfig(), getStatsAggregatorStageConfig());
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationSourceTarget() {
    List<String> lanes = ImmutableList.of("a");
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfiguration("s", "default", "sourceName", 1,
      new ArrayList<Config>(), null, new ArrayList<String>(),
      lanes);
    stages.add(source);
    StageConfiguration target = new StageConfiguration("t", "default", "targetName", 1,
      new ArrayList<Config>(), null, lanes, new ArrayList<String>());
    stages.add(target);
    return new PipelineConfiguration(
        PipelineStoreTask.SCHEMA_VERSION,
        PipelineConfigBean.VERSION,
        UUID.randomUUID(),
        null,
        createPipelineConfigs(),
        null,
        stages,
        getErrorStageConfig(),
        getStatsAggregatorStageConfig()
    );
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationSourceTargetDifferentInstance() {
    List<String> lanes = ImmutableList.of("a");
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfiguration("s1", "default", "sourceName", 1,
      new ArrayList<Config>(), null, new ArrayList<String>(),
      lanes);
    stages.add(source);
    StageConfiguration target = new StageConfiguration("t1", "default", "targetName", 1,
      new ArrayList<Config>(), null, lanes, new ArrayList<String>());
    stages.add(target);
    return new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, PipelineConfigBean.VERSION, UUID.randomUUID(),
                                     null, createPipelineConfigs(), null, stages, getErrorStageConfig(), getStatsAggregatorStageConfig());
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationComplexSourceTarget() {
    List<String> lanes = ImmutableList.of("a");
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfiguration("s", "default", "complexStageName", 1,
      new ArrayList<Config>(), null, new ArrayList<String>(),
      lanes);
    stages.add(source);
    StageConfiguration target = new StageConfiguration("t", "default", "targetName", 1,
      new ArrayList<Config>(), null, lanes, new ArrayList<String>());
    stages.add(target);
    return new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, PipelineConfigBean.VERSION, UUID.randomUUID(),
                                     null, createPipelineConfigs(), null, stages, getErrorStageConfig(), getStatsAggregatorStageConfig());
  }

  public static PipelineConfiguration createPipelineConfigurationSourceTwoTargets() {
    List<String> lanes = ImmutableList.of("a");
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfiguration("s", "default", "sourceName", 1,
      Collections.<Config>emptyList(), null, Collections.<String>emptyList(), lanes);
    stages.add(source);
    StageConfiguration target = new StageConfiguration("t1", "default", "targetName", 1,
      Collections.<Config>emptyList(), null, lanes, Collections.<String>emptyList());
    stages.add(target);
    target = new StageConfiguration("t2", "default", "targetName", 1,
      Collections.<Config>emptyList(), null, lanes, Collections.<String>emptyList());
    stages.add(target);
    return new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, PipelineConfigBean.VERSION, UUID.randomUUID(),
                                     null, createPipelineConfigs(), null, stages, getErrorStageConfig(), getStatsAggregatorStageConfig());
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationWithClusterOnlyStage(ExecutionMode executionMode) {
    List<String> lanes = ImmutableList.of("a");
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfiguration("s", "default", "clusterSource", 1,
                                                       Collections.<Config>emptyList(), null, Collections.<String>emptyList(),
                                                       lanes);
    stages.add(source);
    StageConfiguration target = new StageConfiguration("t", "default", "targetName", 1,
                                                       Collections.<Config>emptyList(), null, lanes, Collections.<String>emptyList());
    stages.add(target);
    return new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, PipelineConfigBean.VERSION, UUID.randomUUID(), null,
                                     Arrays.asList(new Config("executionMode",
                                                                           executionMode.name()), new Config("retryAttempts", 3)), null, stages,
                                     getErrorStageConfig(), getStatsAggregatorStageConfig());
  }

  public static PipelineConfiguration createPipelineWith2OffsetCommitController(ExecutionMode executionMode) {
    List<String> lanes = ImmutableList.of("a", "b");
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfiguration("s", "default", "multiLaneSource", 1,
      Collections.<Config>emptyList(), null, Collections.<String>emptyList(),
      lanes);
    stages.add(source);
    StageConfiguration target1 = new StageConfiguration("t1", "default", "offsetControlTarget", 1,
      Collections.<Config>emptyList(), null, ImmutableList.of("a"), Collections.<String>emptyList());
    stages.add(target1);
    StageConfiguration target2 = new StageConfiguration("t2", "default", "offsetControlTarget", 1,
      Collections.<Config>emptyList(), null, ImmutableList.of("b"), Collections.<String>emptyList());
    stages.add(target2);
    return new PipelineConfiguration(
        PipelineStoreTask.SCHEMA_VERSION,
        PipelineConfigBean.VERSION,
        UUID.randomUUID(),
        null,
        Arrays.asList(
            new Config("executionMode",executionMode.name()),
            new Config("retryAttempts", 3),
            new Config("deliveryGuarantee", DeliveryGuarantee.AT_MOST_ONCE)
        ),
        null,
        stages,
        getErrorStageConfig(),
        getStatsAggregatorStageConfig()
    );
  }

  public static PipelineConfiguration createPipelineWithOffsetCommitController(ExecutionMode executionMode) {
    List<String> lanes = ImmutableList.of("a");
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfiguration("s", "default", "sourceName", 1,
      Collections.<Config>emptyList(), null, Collections.<String>emptyList(),
      lanes);
    stages.add(source);
    StageConfiguration target2 = new StageConfiguration("t2", "default", "offsetControlTarget", 1,
      Collections.<Config>emptyList(), null, ImmutableList.of("a"), Collections.<String>emptyList());
    stages.add(target2);
    return new PipelineConfiguration(
      PipelineStoreTask.SCHEMA_VERSION,
      PipelineConfigBean.VERSION,
      UUID.randomUUID(),
      null,
      Arrays.asList(
        new Config("executionMode",executionMode.name()),
        new Config("retryAttempts", 3),
        new Config("deliveryGuarantee", DeliveryGuarantee.AT_MOST_ONCE)
      ),
      null,
      stages,
      getErrorStageConfig(),
      getStatsAggregatorStageConfig()
    );
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationWithExecutionClusterOnlyStageLibrary(
      String stageInstance, ExecutionMode executionMode) {
    List<String> lanes = ImmutableList.of("a");
    List<StageConfiguration> stages = new ArrayList<>();
    // Stagedef for 'clusterLibrarySource' is created in MockStageLibraryTask
    StageConfiguration source =
      new StageConfiguration(stageInstance, "default", "clusterLibrarySource", 1,
        Collections.<Config> emptyList(), null, Collections.<String> emptyList(), lanes);
    stages.add(source);
    StageConfiguration target =
      new StageConfiguration("t", "default", "targetName", 1, Collections.<Config> emptyList(),
        null, lanes, Collections.<String> emptyList());
    stages.add(target);
    return new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, PipelineConfigBean.VERSION, UUID.randomUUID(), null,
      Arrays.asList(new Config("executionMode", executionMode.name())), null,
      stages, getErrorStageConfig(), getStatsAggregatorStageConfig());
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationWithBothExecutionModeStageLibrary(
      ExecutionMode executionMode) {
    List<String> lanes = ImmutableList.of("a");
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source =
      new StageConfiguration("s", "default", "clusterSource", 1, Collections.<Config> emptyList(),
        null, Collections.<String> emptyList(), lanes);
    stages.add(source);

    // Stagedef for 'commonLibraryTarget' is created in MockStageLibraryTask
    StageConfiguration target =
      new StageConfiguration("t", "default", "commonLibraryTarget", 1,
        Collections.<Config> emptyList(), null, lanes, Collections.<String> emptyList());
    stages.add(target);
    return new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, PipelineConfigBean.VERSION, UUID.randomUUID(),
    null, Arrays.asList(new Config("executionMode", executionMode.name())), null, stages, getErrorStageConfig(), getStatsAggregatorStageConfig());
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfTargetWithReqField() {
    List<String> lanes = ImmutableList.of("a");
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source =
      new StageConfiguration("s", "default", "sourceName", 1, Collections.<Config> emptyList(),
        null, Collections.<String> emptyList(), lanes);
    stages.add(source);

    // Create target with empty value for the required field "requiredFieldConfName".
    //Empty value simulates providing a value and then deleting it
    StageConfiguration target =
      new StageConfiguration("t", "default", "targetWithReqField", 1,
        Arrays.asList(new Config("requiredFieldConfName", "")), null, lanes, Collections.<String> emptyList());
    stages.add(target);
    return new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, PipelineConfigBean.VERSION, UUID.randomUUID(),
      null, Arrays.asList(new Config("executionMode", ExecutionMode.STANDALONE)), null, stages, getErrorStageConfig(), getStatsAggregatorStageConfig());
  }

  public static PipelineConfiguration createPipelineConfigurationSourceTargetWithRequiredFields() {
    List<String> lanes = ImmutableList.of("a");
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfiguration("s", "default", "sourceName", 1,
      new ArrayList<Config>(), null, new ArrayList<String>(),
      lanes);
    stages.add(source);
    StageConfiguration target = new StageConfiguration("t", "default", "targetName", 1,
      Arrays.asList(new Config("stageRequiredFields", Arrays.asList("dummy"))), null, lanes, new ArrayList<String>());
    stages.add(target);
    return new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, PipelineConfigBean.VERSION, UUID.randomUUID(),
      null, createPipelineConfigs(), null, stages, getErrorStageConfig(), getStatsAggregatorStageConfig());
  }

}
