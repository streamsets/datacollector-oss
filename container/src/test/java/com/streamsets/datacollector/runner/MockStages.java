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
package com.streamsets.datacollector.runner;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.classpath.ClasspathValidatorResult;
import com.streamsets.datacollector.cluster.ClusterModeConstants;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.CredentialStoreDefinition;
import com.streamsets.datacollector.config.LineagePublisherDefinition;
import com.streamsets.datacollector.config.ModelDefinition;
import com.streamsets.datacollector.config.ModelType;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineDefinition;
import com.streamsets.datacollector.config.PipelineRulesDefinition;
import com.streamsets.datacollector.config.RawSourceDefinition;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.el.ElConstantDefinition;
import com.streamsets.datacollector.el.ElFunctionDefinition;
import com.streamsets.datacollector.runner.preview.StageConfigurationBuilder;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Executor;
import com.streamsets.pipeline.api.OffsetCommitTrigger;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.RawSourcePreviewer;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.BaseSource;
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
import java.util.UUID;

public class MockStages {

  public static StageConfiguration createPushSource(String instanceName, List<String> outputs) {
    return createPushSource(instanceName, outputs, Collections.<String>emptyList());
  }

  public static StageConfiguration createPushSource(String instanceName, List<String> outputs, List<String> events) {
    return new StageConfigurationBuilder(instanceName, "pushSourceName")
      .withOutputLanes(outputs)
      .withEventLanes(events)
      .build();
  }

  public static StageConfiguration createSource(String instanceName, List<String> outputs) {
    return createSource(instanceName, outputs, Collections.<String>emptyList());
  }

  @SuppressWarnings("unchecked")
  public static StageConfiguration createSource(String instanceName, List<String> outputs, List<String> events) {
    return new StageConfigurationBuilder(instanceName, "sourceName")
      .withOutputLanes(outputs)
      .withEventLanes(events)
      .build();
  }

  public static StageConfiguration createProcessor(String instanceName, List<String> inputs, List<String> outputs) {
    return createProcessor(instanceName, inputs, outputs, Collections.<String>emptyList());
  }

  @SuppressWarnings("unchecked")
  public static StageConfiguration createProcessor(String instanceName, List<String> inputs, List<String> outputs, List<String> events) {
    return new StageConfigurationBuilder(instanceName, "processorName")
      .withInputLanes(inputs)
      .withOutputLanes(outputs)
      .withEventLanes(events)
      .build();
  }

  public static StageConfiguration createTarget(String instanceName, List<String> inputs) {
    return createTarget(instanceName, inputs, Collections.<String>emptyList());
  }

  @SuppressWarnings("unchecked")
  public static StageConfiguration createTarget(String instanceName, List<String> inputs, List<String> events) {
    return new StageConfigurationBuilder(instanceName, "targetName")
      .withInputLanes(inputs)
      .withEventLanes(events)
      .build();
  }

  @SuppressWarnings("unchecked")
  public static StageConfiguration createExecutor(String instanceName, List<String> inputs, List<String> events) {
    return new StageConfigurationBuilder(instanceName, "executorName")
      .withInputLanes(inputs)
      .withEventLanes(events)
      .build();
  }

  private static Source sourceCapture;
  private static PushSource pushSourceCapture;
  private static Processor processorCapture;
  private static Target targetCapture;
  private static Executor executorCapture;
  private static Target errorCapture;

  // it must be called after the pipeline is built
  public static void setSourceCapture(Source s) {
    sourceCapture = s;
  }

  public static Source getSourceCapture() {
    return sourceCapture;
  }

  // it must be called after the pipeline is built
  public static void setPushSourceCapture(PushSource s) {
    pushSourceCapture = s;
  }

  public static PushSource getPushSourceCapture() {
    return pushSourceCapture;
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

  // it must be called after the pipeline is built
  public static void setExecutorCapture(Executor e) {
    executorCapture = e;
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

  public static class MPushSource implements PushSource {

    @Override
    public int getNumberOfThreads() {
      if(pushSourceCapture != null) {
        return pushSourceCapture.getNumberOfThreads();
      }

      return 1;
    }

    @Override
    public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {
      if(pushSourceCapture != null) {
        pushSourceCapture.produce(lastOffsets, maxBatchSize);
      }
    }

    @Override
    public List<ConfigIssue> init(Info info, Context context) {
      if(pushSourceCapture != null) {
        return pushSourceCapture.init(info, context);
      }

      return Collections.emptyList();
    }

    @Override
    public void destroy() {
      if(pushSourceCapture != null) {
        pushSourceCapture.destroy();
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
    public Object put(List<Map.Entry> batch) throws InterruptedException {
      return null;
    }

    @Override
    public void completeBatch() throws InterruptedException {

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

  public static class MExecutor implements Executor {

    @Override
    public List<ConfigIssue> init(Info info, Context context) {
      if (executorCapture != null) {
        return executorCapture.init(info, context);
      } else {
        return Collections.emptyList();
      }
    }

    @Override
    public void destroy() {
      if (executorCapture != null) {
        executorCapture.destroy();
      }
    }

    @Override
    public void write(Batch batch) throws StageException {
      if (executorCapture != null) {
        executorCapture.write(batch);
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

  public static class OffsetControllerSource extends BaseSource implements OffsetCommitTrigger {

    @Override
    public boolean commit() {
      return false;
    }

    @Override
    public String produce(String s, int i, BatchMaker batchMaker) throws StageException {
      return null;
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

  public static StageLibraryTask createClusterMapRStreamingStageLibrary(ClassLoader cl) {
    return new MockStageLibraryTask.ClusterMapRStreamingBuilder(cl).build();
  }

  public static StageLibraryTask createClusterBatchStageLibrary(ClassLoader cl) {
    return new MockStageLibraryTask.ClusterBatchBuilder(cl).build();
  }


  public static StageLibraryTask createStageLibrary() {
    return createStageLibrary(Thread.currentThread().getContextClassLoader());
  }

  public static UserContext userContext() {
    return new UserContext("test-user");
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
    public PipelineRulesDefinition getPipelineRules() {
      return PipelineRulesDefinition.getPipelineRulesDef();
    }

    @Override
    public List<StageDefinition> getStages() {
      return stages;
    }

    @Override
    public List<LineagePublisherDefinition> getLineagePublisherDefinitions() {
      return Collections.emptyList();
    }

    @Override
    public LineagePublisherDefinition getLineagePublisherDefinition(String library, String name) {
      return null;
    }

    @Override
    public List<CredentialStoreDefinition> getCredentialStoreDefinitions() {
      return Collections.emptyList();
    }

    @Override
    public List<ServiceDefinition> getServiceDefinitions() {
      return Collections.emptyList();
    }

    @Override
    public ServiceDefinition getServiceDefinition(Class serviceInterface, boolean forExecution) {
      return null;
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
    public List<ClasspathValidatorResult> validateStageLibClasspath() {
      return Collections.emptyList();
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

        StageDefinition sDef = new StageDefinitionBuilder(cl, MSource.class, "sourceName")
          .withRawSourceDefintion(rawSourceDefinition)
          .build();
        StageDefinition socDef = new StageDefinitionBuilder(cl, MSourceOffsetCommitter.class, "sourceOffsetCommitterName")
          .build();
        // Event producing source
        StageDefinition seDef = new StageDefinitionBuilder(cl, MSource.class, "sourceNameEvent")
          .withProducingEvents(true)
          .build();
        StageDefinition pushSourceDef = new StageDefinitionBuilder(cl, MPushSource.class, "pushSourceName")
          .withProducingEvents(true)
          .build();


        StageDefinition pDef = new StageDefinitionBuilder(cl, MProcessor.class, "processorName")
          .build();

        ModelDefinition m = new ModelDefinition(ModelType.FIELD_SELECTOR_MULTI_VALUE, null, Collections.<String>emptyList(),
          Collections.<String>emptyList(), null, null, null);
        ConfigDefinition stageReqField = new ConfigDefinition("stageRequiredFields", ConfigDef.Type.MODEL, "stageRequiredFields",
          "stageRequiredFields", null, false, "groupName", "stageRequiredFieldName", m, "", null, 0, Collections.<ElFunctionDefinition>emptyList(),
          Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<Class> emptyList(),
          ConfigDef.Evaluation.IMPLICIT, new HashMap<String, List<Object>>());

        StageDefinition tDef = new StageDefinitionBuilder(cl, MTarget.class, "targetName")
          .withConfig(stageReqField)
          .withExecutionModes(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH, ExecutionMode.CLUSTER_MESOS_STREAMING)
          .build();

        StageDefinition tEventDef = new StageDefinitionBuilder(cl, MExecutor.class, "executorName")
          .withConfig(stageReqField)
          .withProducingEvents(true)
          .withPipelineLifecycleStage(true)
          .withExecutionModes(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH, ExecutionMode.CLUSTER_MESOS_STREAMING)
          .build();

        StageDefinition teDef = new StageDefinitionBuilder(cl, MTarget.class, "targetNameEvent")
          .withProducingEvents(true)
          .build();

        ConfigDefinition reqField = new ConfigDefinition(
          "requiredFieldConfName", ConfigDef.Type.STRING, "requiredFieldLabel", "requiredFieldDesc", 10, true,
          "groupName", "requiredFieldFieldName", null, "", null, 0, Collections.<ElFunctionDefinition>emptyList(),
          Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<Class> emptyList(),
          ConfigDef.Evaluation.IMPLICIT, new HashMap<String, List<Object>>());

        StageDefinition targetWithReqField = new StageDefinitionBuilder(cl, MTarget.class, "targetWithReqField")
          .withConfig(reqField)
          .build();

        ConfigDefinition requiredMapField = new ConfigDefinition(
            "requiredMapFieldConfName",
            ConfigDef.Type.MAP,
            "requiredMapFieldLabel",
            "requiredMapFieldDesc",
            new ArrayList<>(),
            true,
            "groupName",
            "requiredMapFieldFieldName",
            null,
            "",
            null,
            0,
            Collections.<ElFunctionDefinition>emptyList(),
            Collections.<ElConstantDefinition>emptyList(),
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            "text/plain",
            0,
            Collections.<Class> emptyList(),
            ConfigDef.Evaluation.IMPLICIT,
            new HashMap<>()
        );

        StageDefinition targetWithRequiredMapField = new StageDefinitionBuilder(cl, MTarget.class, "targetWithRequiredMapField")
            .withConfig(requiredMapField)
            .build();

        //error target configurations
        ConfigDefinition errorTargetConf = new ConfigDefinition(
          "errorTargetConfName", ConfigDef.Type.STRING, "errorTargetConfLabel", "errorTargetConfDesc",
          "/SDC_HOME/errorDir", true, "groupName", "errorTargetConfFieldName", null, "", null , 0,
          Collections.<ElFunctionDefinition>emptyList(), Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0,
          Collections.<Class> emptyList(), ConfigDef.Evaluation.IMPLICIT, new HashMap<String, List<Object>>());

        StageDefinition eDef = new StageDefinitionBuilder(cl, ETarget.class, "errorTarget")
          .withErrorStage(true)
          .withPreconditions(false)
          .withConfig(errorTargetConf)
          .withExecutionModes(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH, ExecutionMode.CLUSTER_MESOS_STREAMING)
          .build();

        StageDefinition statsDef = new StageDefinitionBuilder(cl, StatsTarget.class, "statsAggregator")
          .withPreconditions(false)
          .withStatsAggregatorStage(true)
          .withExecutionModes(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH, ExecutionMode.CLUSTER_MESOS_STREAMING)
          .build();

        ConfigDefinition depConfDef = new ConfigDefinition(
          "dependencyConfName", ConfigDef.Type.NUMBER, "dependencyConfLabel", "dependencyConfDesc", 5, true,
          "groupName", "dependencyConfFieldName", null, "", null, 0, Collections.<ElFunctionDefinition>emptyList(),
          Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<Class> emptyList(),
          ConfigDef.Evaluation.IMPLICIT, new HashMap<String, List<Object>>());
        List<Object> triggeredBy = new ArrayList<>();
        triggeredBy.add(1);
        Map<String, List<Object>> triggered = new HashMap<>(1);
        List<Object> triggerValues = new ArrayList<Object>();
        triggerValues.add(1);
        triggered.put("dependencyConfName", triggerValues);
        ConfigDefinition triggeredConfDef = new ConfigDefinition(
          "triggeredConfName", ConfigDef.Type.NUMBER, "triggeredConfLabel", "triggeredConfDesc", 10, true,
          "groupName", "triggeredConfFieldName", null, "dependencyConfName", triggeredBy, 0,
          Collections.<ElFunctionDefinition>emptyList(), Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0,
          Collections.<Class> emptyList(), ConfigDef.Evaluation.IMPLICIT, triggered);

        StageDefinition swcDef = new StageDefinitionBuilder(cl, MSource.class, "sourceWithConfigsName")
          .withConfig(depConfDef, triggeredConfDef)
          .build();

        StageDefinition clusterStageDef = new StageDefinitionBuilder(cl, ClusterMSource.class, "clusterSource")
          .withExecutionModes(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.CLUSTER_BATCH, ExecutionMode.CLUSTER_MESOS_STREAMING)
          .build();

        StageDefinition clusterLibraryStageDef = new StageDefinitionBuilder(cl, ClusterMSource.class, "clusterLibrarySource")
          .withExecutionModes(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.CLUSTER_BATCH)
          .build();

        StageDefinition commonLibraryTargetDef = new StageDefinitionBuilder(cl, MTarget.class, "commonLibraryTarget")
          .build();

        ConfigDefinition regularConf = new ConfigDefinition(
          "regularConfName", ConfigDef.Type.NUMBER, "regularConfLabel", "regularConfDesc", 10, true,
          "groupName", "regularConfFieldName", null, "", null, 0, Collections.<ElFunctionDefinition>emptyList(),
          Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<Class> emptyList(),
          ConfigDef.Evaluation.IMPLICIT, new HashMap<String, List<Object>>());

        List<ConfigDefinition> list = new ArrayList<>();
        list.add(regularConf);
        ModelDefinition modelDefinition = new ModelDefinition(ModelType.LIST_BEAN, null, Collections.<String>emptyList(),
          Collections.<String>emptyList(), null, list, null);

        ConfigDefinition complexConf = new ConfigDefinition(
          "complexConfName", ConfigDef.Type.MODEL, "complexConfLabel", "complexConfDesc", null, true,
          "groupName", "complexConfFieldName", modelDefinition, "", null, 0, Collections.<ElFunctionDefinition>emptyList(),
          Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<Class> emptyList(),
          ConfigDef.Evaluation.IMPLICIT, new HashMap<String, List<Object>>());

        StageDefinition complexStage = new StageDefinitionBuilder(cl,ComplexSource.class, "complexStageName")
          .withConfig(complexConf)
          .build();

        StageDefinition offsetControlTarget = new StageDefinitionBuilder(cl, OffsetControllerTarget.class, "offsetControlTarget")
          .withOffsetCommitTrigger(true)
          .build();


        StageDefinition multiLaneSource = new StageDefinitionBuilder(cl, OffsetControllerSource.class, "multiLaneSource")
          .withOutputStreams(2)
          .build();

        StageDefinition[] stageDefs =
          new StageDefinition[] {
              sDef,
              socDef,
              seDef,
              pushSourceDef,
              pDef,
              tDef,
              tEventDef,
              teDef,
              targetWithReqField,
              targetWithRequiredMapField,
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
      public Builder overrideClass(String name, Class<? extends Stage> klass) {
        if (stages.containsKey(name)) {
          StageDefinition oldDef = stages.get(name);
          StageDefinition newDef = new StageDefinition(
              StageDefinitionBuilder.createLibraryDef(klass.getClassLoader()),
              false,
              klass,
              oldDef.getName(),
              oldDef.getVersion(),
              oldDef.getLabel(),
              oldDef.getDescription(),
              oldDef.getType(),
              oldDef.isErrorStage(),
              oldDef.hasPreconditions(),
              oldDef.hasOnRecordError(),
              oldDef.getConfigDefinitions(),
              oldDef.getRawSourceDefinition(),
              oldDef.getIcon(),
              oldDef.getConfigGroupDefinition(),
              oldDef.isVariableOutputStreams(),
              oldDef.getOutputStreams(),
              oldDef.getOutputStreamLabelProviderClass(),
              Arrays.asList(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE),
              false,
              new StageUpgrader.Default(),
              Collections.<String>emptyList(),
              false,
              "",
              false,
              false,
              false,
              false,
              Collections.emptyList()
          );
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

      return new StageDefinitionBuilder(cl, ETarget.class, "errorTarget")
        .withErrorStage(true)
        .withPreconditions(false)
        .withConfig(errorTargetConf)
        .withExecutionModes(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH, ExecutionMode.CLUSTER_MESOS_STREAMING)
        .build();
    }

    public static StageDefinition getStatsAggStageDefinition(ClassLoader cl) {
      return new StageDefinitionBuilder(cl, StatsTarget.class, "statsAggregator")
          .withPreconditions(false)
          .withStatsAggregatorStage(true)
          .withExecutionModes(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH)
          .build();
    }

    public static class ClusterStreamingBuilder {
      private final StageDefinition clusterStageDef;
      private final StageDefinition errorTargetStageDef;
      private final StageDefinition statsTargetStageDef;

      public ClusterStreamingBuilder() {
        this(Thread.currentThread().getContextClassLoader());
      }

      public ClusterStreamingBuilder(ClassLoader cl) {
        clusterStageDef = new StageDefinitionBuilder(cl, MSource.class, "sourceName")
          .withExecutionModes(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.CLUSTER_BATCH, ExecutionMode.CLUSTER_MESOS_STREAMING)
          .withRawSourceDefintion(getRawSourceDefinition())
          .withLibJarsRegexp(ClusterModeConstants.SPARK_KAFKA_JAR_REGEX)
          .build();

        errorTargetStageDef = getErrorStageDefinition(cl);
        statsTargetStageDef = getStatsAggStageDefinition(cl);
      }


      public StageLibraryTask build() {
        return new MockStageLibraryTask(ImmutableList.of(clusterStageDef, errorTargetStageDef, statsTargetStageDef));
      }
    }


    public static class ClusterMapRStreamingBuilder {
      private final StageDefinition clusterStageDef;
      private final StageDefinition errorTargetStageDef;
      private final StageDefinition statsTargetStageDef;

      public ClusterMapRStreamingBuilder() {
        this(Thread.currentThread().getContextClassLoader());
      }

      public ClusterMapRStreamingBuilder(ClassLoader cl) {
        clusterStageDef = new StageDefinitionBuilder(cl, MSource.class, "sourceName")
            .withExecutionModes(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE)
            .withRawSourceDefintion(getRawSourceDefinition())
            .withLibJarsRegexp("maprfs-\\d+.*")
            .build();

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
        clusterStageDef = new StageDefinitionBuilder(cl, MSource.class, "sourceName")
          .withExecutionModes(ExecutionMode.CLUSTER_BATCH, ExecutionMode.STANDALONE)
          .withRawSourceDefintion(getRawSourceDefinition())
          .withLibJarsRegexp(ClusterModeConstants.AVRO_JAR_REGEX, ClusterModeConstants.AVRO_MAPRED_JAR_REGEX)
          .build();
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
    StageConfiguration source = new StageConfigurationBuilder("s", "sourceName")
      .withOutputLanes("s")
      .build();
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
    return new StageConfigurationBuilder("errorStage", "errorTarget")
      .withConfig(new Config("errorTargetConfName", "/SDC_HOME/errorDir"))
      .build();
  }

  public static StageConfiguration getStatsAggregatorStageConfig() {
    return new StageConfigurationBuilder("statsAggregator", "statsAggregator").build();
  }

  public static StageConfiguration getLifecycleExecutorConfig() {
    return new StageConfigurationBuilder("eventStage", "executorName")
      .build();
  }

  private static List<Config> createPipelineConfigs() {
    List<Config> pipelineConfig = new ArrayList<>();
    pipelineConfig.add(new Config("executionMode", ExecutionMode.STANDALONE.name()));
    pipelineConfig.add(new Config("deliveryGuarantee", DeliveryGuarantee.AT_LEAST_ONCE.name()));
    return pipelineConfig;
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationSourceProcessorTarget(int schemaVersion) {
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfigurationBuilder("s", "sourceName")
      .withOutputLanes("s")
      .build();
    stages.add(source);
    StageConfiguration processor = new StageConfigurationBuilder("p", "processorName")
      .withInputLanes("s")
      .withOutputLanes("p")
      .build();
    stages.add(processor);
    StageConfiguration target = new StageConfigurationBuilder("t", "targetName")
      .withInputLanes("p")
      .build();
    stages.add(target);

    PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(schemaVersion,
        PipelineConfigBean.VERSION,
        "pipelineId",
        UUID.randomUUID(),
        "label",
        null,
        createPipelineConfigs(),
        null,
        stages,
        getErrorStageConfig(),
        getStatsAggregatorStageConfig(),
        Collections.emptyList(),
        Collections.emptyList()
    );
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("a", "A");
    pipelineConfiguration.setMetadata(metadata);
    return pipelineConfiguration;
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationSourceTargetWithEventsOpen() {
    List<StageConfiguration> stages = new ArrayList<>();

    StageConfiguration source = new StageConfigurationBuilder("s", "sourceNameEvent")
      .withOutputLanes("t")
      .build();
    stages.add(source);
    StageConfiguration target = new StageConfigurationBuilder("t", "targetName")
      .withInputLanes("t")
      .build();
    stages.add(target);

    PipelineConfiguration pipelineConfiguration = pipeline(stages);
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("a", "A");
    pipelineConfiguration.setMetadata(metadata);
    return pipelineConfiguration;
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationSourceTargetWithEventsProcessedUnsorted() {
    List<StageConfiguration> stages = new ArrayList<>();

    StageConfiguration eventDest = new StageConfigurationBuilder("e", "targetName")
      .withInputLanes("e")
      .build();
    stages.add(eventDest);
    StageConfiguration source = new StageConfigurationBuilder("s", "sourceNameEvent")
      .withOutputLanes("t")
      .withEventLanes("e")
      .build();
    stages.add(source);
    StageConfiguration target = new StageConfigurationBuilder("t", "targetName")
      .withInputLanes("t")
      .build();
    stages.add(target);

    PipelineConfiguration pipelineConfiguration = pipeline(stages);
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("a", "A");
    pipelineConfiguration.setMetadata(metadata);
    return pipelineConfiguration;
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationSourceTargetWithEventsProcessed() {
    List<StageConfiguration> stages = new ArrayList<>();

    StageConfiguration source = new StageConfigurationBuilder("s", "sourceNameEvent")
      .withOutputLanes("t")
      .withEventLanes("e")
      .build();
    stages.add(source);
    StageConfiguration executor = new StageConfigurationBuilder("e", "executorName")
      .withInputLanes("e")
      .build();
    stages.add(executor);
    StageConfiguration target = new StageConfigurationBuilder("t", "targetName")
      .withInputLanes("t")
      .build();
    stages.add(target);

    PipelineConfiguration pipelineConfiguration = pipeline(stages);
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("a", "A");
    pipelineConfiguration.setMetadata(metadata);
    return pipelineConfiguration;
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationSourceTargetDeclaredEventLaneWithoutSupportingEvents() {
    List<StageConfiguration> stages = new ArrayList<>();

    StageConfiguration source = new StageConfigurationBuilder("s", "sourceName")
      .withOutputLanes("t")
      .withEventLanes("e")
      .build();
    stages.add(source);
    StageConfiguration target = new StageConfigurationBuilder("t", "targetName")
      .withInputLanes("t")
      .build();
    stages.add(target);

    PipelineConfiguration pipelineConfiguration = pipeline(stages);
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("a", "A");
    pipelineConfiguration.setMetadata(metadata);
    return pipelineConfiguration;
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationSourceProcessorTargetWithMergingEventAndDataLane() {
    List<StageConfiguration> stages = new ArrayList<>();

    StageConfiguration eventDest = new StageConfigurationBuilder("p", "processorName")
      .withInputLanes("e")
      .withOutputLanes("eo")
      .build();
    stages.add(eventDest);
    StageConfiguration source = new StageConfigurationBuilder("s", "sourceNameEvent")
      .withOutputLanes("t")
      .withEventLanes("e")
      .build();
    stages.add(source);
    StageConfiguration target = new StageConfigurationBuilder("t", "targetName")
      .withInputLanes("t", "eo")
      .build();
    stages.add(target);

    PipelineConfiguration pipelineConfiguration = pipeline(stages);
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("a", "A");
    pipelineConfiguration.setMetadata(metadata);
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

    StageConfiguration source = new StageConfigurationBuilder("s", "sourceName")
      .withOutputLanes("s")
      .build();
    stages.add(source);

    StageConfiguration processor1 = new StageConfigurationBuilder("p1", "processorName")
      .withInputLanes("s")
      .withOutputLanes("p1")
      .build();
    stages.add(processor1);

    StageConfiguration processor2 = new StageConfigurationBuilder("p2", "processorName")
      .withInputLanes("s")
      .withOutputLanes("p2")
      .build();
    stages.add(processor2);

    StageConfiguration processor3 = new StageConfigurationBuilder("p3", "processorName")
      .withInputLanes("s")
      .withOutputLanes("p3")
      .build();
    stages.add(processor3);

    StageConfiguration processor4 = new StageConfigurationBuilder("p4", "processorName")
      .withInputLanes("p1")
      .withOutputLanes("p4")
      .build();
    stages.add(processor4);

    StageConfiguration processor5 = new StageConfigurationBuilder("p5", "processorName")
      .withInputLanes("p2")
      .withOutputLanes("p5")
      .build();
    stages.add(processor5);

    StageConfiguration processor6 = new StageConfigurationBuilder("p6", "processorName")
      .withInputLanes("p2", "p3")
      .withOutputLanes("p6")
      .build();
    stages.add(processor6);


    StageConfiguration target = new StageConfigurationBuilder("t", "targetName")
      .withInputLanes("p4", "p5", "p6")
      .build();
    stages.add(target);

    return new PipelineConfiguration(
        schemaVersion,
        PipelineConfigBean.VERSION,
        "pipelineId",
        UUID.randomUUID(),
        "label",
        null,
        createPipelineConfigs(),
        null,
        stages,
        getErrorStageConfig(),
        getStatsAggregatorStageConfig(),
        Collections.emptyList(),
        Collections.emptyList()
    );
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationSourceOffsetCommitterProcessorTarget() {
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfigurationBuilder("s", "sourceOffsetCommitterName")
      .withOutputLanes("s")
      .build();
    stages.add(source);
    StageConfiguration processor = new StageConfigurationBuilder("p", "processorName")
      .withInputLanes("s")
      .withOutputLanes("p")
      .build();
    stages.add(processor);
    StageConfiguration target = new StageConfigurationBuilder("t", "targetName")
      .withInputLanes("p")
      .build();
    stages.add(target);
    return pipeline(stages);
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineWithRequiredDependentConfig() {
    List<StageConfiguration> stages = new ArrayList<>();

    StageConfiguration source = new StageConfigurationBuilder("s", "sourceWithConfigsName")
      .withOutputLanes("a")
      .build();
    stages.add(source);
    StageConfiguration target = new StageConfigurationBuilder("t", "targetName")
      .withInputLanes("a")
      .build();
    stages.add(target);
    return pipeline(stages);
  }

  public static PipelineConfiguration createPipelineConfigurationWithLabels(List<String> labels) {
    PipelineConfiguration pipelineConfiguration = pipeline(null);

    pipelineConfiguration.setPipelineInfo(
        new PipelineInfo(
            "label",
            "label",
            "xyz description",
            new java.util.Date(0),
            new java.util.Date(0),
            "creator",
            "lastModifier",
            "1", UUID.randomUUID(),
            true,
            null,
            "2.5",
            "x"
        )
    );

    Map<String, Object> metadata = new HashMap<>();
    metadata.put("labels", labels);
    pipelineConfiguration.setMetadata(metadata);
    return pipelineConfiguration;
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationSourceTarget() {
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfigurationBuilder("s", "sourceName")
      .withOutputLanes("a")
      .build();
    stages.add(source);
    StageConfiguration target = new StageConfigurationBuilder("t", "targetName")
      .withInputLanes("a")
      .build();
    stages.add(target);
    return pipeline(stages);
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationPushSourceTarget() {
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfigurationBuilder("s", "pushSourceName")
      .withOutputLanes("a")
      .build();
    stages.add(source);
    StageConfiguration target = new StageConfigurationBuilder("t", "targetName")
      .withInputLanes("a")
      .build();
    stages.add(target);
    return pipeline(stages);
  }

  public static PipelineConfiguration createPipelineConfigurationSourceTwoTargets() {
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfigurationBuilder("s", "sourceName")
      .withOutputLanes("a")
      .build();
    stages.add(source);
    StageConfiguration target = new StageConfigurationBuilder("t1", "targetName")
      .withInputLanes("a")
      .build();
    stages.add(target);
    target = new StageConfigurationBuilder("t2", "targetName")
      .withInputLanes("a")
      .build();
    stages.add(target);
    return pipeline(stages);
  }

  public static PipelineConfiguration createPipelineConfigurationSourceTwoTargetsTwoEvents() {
    List<StageConfiguration> stages = new ArrayList<>();

    StageConfiguration source = new StageConfigurationBuilder("s", "sourceNameEvent")
      .withOutputLanes("t")
      .withEventLanes("e")
      .build();
    stages.add(source);
    StageConfiguration target = new StageConfigurationBuilder("t1", "targetName")
      .withInputLanes("t")
      .build();
    stages.add(target);
    target = new StageConfigurationBuilder("t2", "targetName")
      .withInputLanes("t")
      .build();
    stages.add(target);
    target = new StageConfigurationBuilder("t3", "targetName")
      .withInputLanes("e")
      .build();
    stages.add(target);
    target = new StageConfigurationBuilder("t4", "targetName")
      .withInputLanes("e")
      .build();
    stages.add(target);
    return pipeline(stages);
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationLifecycleEvents() {
    StageConfiguration source = new StageConfigurationBuilder("s", "sourceNameEvent")
      .withOutputLanes("t")
      .build();
    StageConfiguration target = new StageConfigurationBuilder("t", "targetNameEvent")
      .withInputLanes("t")
      .build();

    StageConfiguration startEvent = new StageConfigurationBuilder("start", "executorName")
      .build();

    StageConfiguration stopEvent = new StageConfigurationBuilder("stop", "executorName")
      .build();

    PipelineConfiguration pipelineConfiguration = pipeline(
      ImmutableList.of(target, source),
      ImmutableList.of(startEvent),
      ImmutableList.of(stopEvent)
    );
    return pipelineConfiguration;
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationLifecycleEventsIncorrect() {
    StageConfiguration source = new StageConfigurationBuilder("s", "sourceNameEvent")
      .withOutputLanes("t")
      .build();
    StageConfiguration target = new StageConfigurationBuilder("t", "targetNameEvent")
      .withInputLanes("t")
      .build();

    StageConfiguration startEvent = new StageConfigurationBuilder("start", "executorName")
      .withInputLanes("startInput")
      .build();

    StageConfiguration stopEvent = new StageConfigurationBuilder("stop", "executorName")
      .withEventLanes("stopEvent")
      .build();

    PipelineConfiguration pipelineConfiguration = pipeline(
      ImmutableList.of(target, source),
      ImmutableList.of(startEvent),
      ImmutableList.of(stopEvent)
    );
    return pipelineConfiguration;
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationWithClusterOnlyStage(ExecutionMode executionMode) {
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfigurationBuilder("s", "clusterSource")
      .withOutputLanes("a")
      .build();
    stages.add(source);
    StageConfiguration target = new StageConfigurationBuilder("t", "targetName")
      .withInputLanes("a")
      .build();
    stages.add(target);
    return new PipelineConfiguration(
        PipelineStoreTask.SCHEMA_VERSION,
        PipelineConfigBean.VERSION,
        "pipelineId",
        UUID.randomUUID(),
        "label",
        null,
        Arrays.asList(
          new Config("executionMode", executionMode.name()),
          new Config("retryAttempts", 3)
        ),
        null,
        stages,
        getErrorStageConfig(),
        getStatsAggregatorStageConfig(),
        Collections.emptyList(),
        Collections.emptyList()
    );
  }

  public static PipelineConfiguration createPipelineWith2OffsetCommitController(ExecutionMode executionMode) {
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfigurationBuilder("s", "multiLaneSource")
      .withOutputLanes("a", "b")
      .build();
    stages.add(source);
    StageConfiguration target1 = new StageConfigurationBuilder("t1", "offsetControlTarget")
      .withInputLanes("a")
      .build();
    stages.add(target1);
    StageConfiguration target2 = new StageConfigurationBuilder("t2", "offsetControlTarget")
      .withInputLanes("b")
      .build();
    stages.add(target2);
    return new PipelineConfiguration(
        PipelineStoreTask.SCHEMA_VERSION,
        PipelineConfigBean.VERSION,
        "pipelineId",
        UUID.randomUUID(),
        "label",
        null,
        Arrays.asList(
            new Config("executionMode",executionMode.name()),
            new Config("retryAttempts", 3),
            new Config("deliveryGuarantee", DeliveryGuarantee.AT_MOST_ONCE)
        ),
        null,
        stages,
        getErrorStageConfig(),
        getStatsAggregatorStageConfig(),
        Collections.emptyList(),
        Collections.emptyList()
    );
  }

  public static PipelineConfiguration createPipelineWithOffsetCommitController(ExecutionMode executionMode) {
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfigurationBuilder("s", "sourceName")
      .withOutputLanes("a")
      .build();
    stages.add(source);
    StageConfiguration target2 = new StageConfigurationBuilder("t2", "offsetControlTarget")
      .withInputLanes("a")
      .build();
    stages.add(target2);
    return new PipelineConfiguration(
      PipelineStoreTask.SCHEMA_VERSION,
      PipelineConfigBean.VERSION,
        "pipelineId",
      UUID.randomUUID(),
        "label",
        null,
      Arrays.asList(
        new Config("executionMode",executionMode.name()),
        new Config("retryAttempts", 3),
        new Config("deliveryGuarantee", DeliveryGuarantee.AT_MOST_ONCE)
      ),
      null,
      stages,
      getErrorStageConfig(),
      getStatsAggregatorStageConfig(),
      Collections.emptyList(),
      Collections.emptyList()
    );
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfTargetWithReqField() {
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfigurationBuilder("s", "sourceName")
      .withOutputLanes("a")
      .build();
    stages.add(source);

    // Create target with empty value for the required field "requiredFieldConfName".
    //Empty value simulates providing a value and then deleting it
    StageConfiguration target = new StageConfigurationBuilder("t", "targetWithReqField")
      .withConfig(new Config("requiredFieldConfName", ""))
      .withInputLanes("a")
      .build();
    stages.add(target);
    return pipeline(stages);
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfTargetWithRequiredMapField() {
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfigurationBuilder("s", "sourceName")
        .withOutputLanes("a")
        .build();
    stages.add(source);

    // Create target with empty value for the required Map field "requiredMapFieldConfName"
    // Default "empty" value deserialized from our JSON is an empty ArrayList
    StageConfiguration target = new StageConfigurationBuilder("t", "targetWithRequiredMapField")
        .withConfig(new Config("requiredMapFieldConfName", new ArrayList<>()))
        .withInputLanes("a")
        .build();
    stages.add(target);
    return pipeline(stages);
  }

  public static PipelineConfiguration createPipelineConfigurationSourceTargetWithRequiredFields() {
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfigurationBuilder("s", "sourceName")
      .withOutputLanes("a")
      .build();
    stages.add(source);
    StageConfiguration target = new StageConfigurationBuilder("t", "targetName")
      .withConfig(new Config("stageRequiredFields", Arrays.asList("dummy")))
      .withInputLanes("a")
      .build();
    stages.add(target);
    return pipeline(stages);
  }

  private static PipelineConfiguration pipeline(List<StageConfiguration> stages) {
    return pipeline(stages, Collections.emptyList(), Collections.emptyList());
  }

  private static PipelineConfiguration pipeline(
    List<StageConfiguration> stages,
    List<StageConfiguration> startStages,
    List<StageConfiguration> stopStages
  ) {
    return new PipelineConfiguration(
        PipelineStoreTask.SCHEMA_VERSION,
        PipelineConfigBean.VERSION,
        "pipelineId",
        UUID.randomUUID(),
        "label",
        null,
        createPipelineConfigs(),
        null,
        stages,
        getErrorStageConfig(),
        getStatsAggregatorStageConfig(),
        startStages,
        stopStages
    );
  }
}
