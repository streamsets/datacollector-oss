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
package com.streamsets.datacollector.creation;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.InterceptorDefinition;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineFragmentConfiguration;
import com.streamsets.datacollector.config.PipelineGroups;
import com.streamsets.datacollector.config.PipelineWebhookConfig;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.ServiceConfiguration;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.SparkClusterType;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.definition.StageDefinitionExtractor;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.InterceptorCreatorContextBuilder;
import com.streamsets.datacollector.stagelibrary.ClassLoaderReleaser;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.IssueCreator;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.interceptor.Interceptor;
import com.streamsets.pipeline.api.interceptor.InterceptorCreator;
import com.streamsets.pipeline.api.service.Service;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class PipelineBeanCreator {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineBeanCreator.class);

  public static final String PIPELINE_LIB_DEFINITION = "Pipeline";
  public static final String FRAGMENT_LIB_DEFINITION = "Fragment";
  private static final String RULE_DEFINITIONS_LIB_DEFINITION = "RuleDefinitions";
  private static final String PARAMETERS = "constants";

  private static final PipelineBeanCreator CREATOR = new PipelineBeanCreator() {
  };

  public static PipelineBeanCreator get() {
    return CREATOR;
  }

  public static void prepareForConnections(Configuration configuration, RuntimeInfo runtimeInfo) {
    ConfigInjector.prepareForConnections(configuration, runtimeInfo);
  }

  public static final StageDefinition PIPELINE_DEFINITION = getPipelineDefinition();

  static private StageDefinition getPipelineDefinition() {
    StageLibraryDefinition libraryDef = new StageLibraryDefinition(
        Thread.currentThread().getContextClassLoader(),
        PIPELINE_LIB_DEFINITION,
        PIPELINE_LIB_DEFINITION,
        new Properties(),
        null,
        null,
        null
    );
    return StageDefinitionExtractor.get()
        .extract(libraryDef, PipelineConfigBean.class, "Pipeline Config Definitions");
  }

  public static final StageDefinition RULES_DEFINITION = getRulesDefinition();

  static private StageDefinition getRulesDefinition() {
    StageLibraryDefinition libraryDef = new StageLibraryDefinition(
        Thread.currentThread().getContextClassLoader(),
        RULE_DEFINITIONS_LIB_DEFINITION,
        RULE_DEFINITIONS_LIB_DEFINITION,
        new Properties(),
        null,
        null,
        null
    );
    return StageDefinitionExtractor.get().extract(
        libraryDef,
        RuleDefinitionsConfigBean.class,
        "Rules Definitions Config Definitions"
    );
  }

  public static final StageDefinition FRAGMENT_DEFINITION = getFragmentDefinition();

  static private StageDefinition getFragmentDefinition() {
    StageLibraryDefinition libraryDef = new StageLibraryDefinition(
        Thread.currentThread().getContextClassLoader(),
        FRAGMENT_LIB_DEFINITION,
        FRAGMENT_LIB_DEFINITION,
        new Properties(),
        null,
        null,
        null
    );
    return StageDefinitionExtractor.get()
        .extract(libraryDef, PipelineFragmentConfigBean.class, "Fragment Config Definitions");
  }

  public PipelineConfigBean create(
      PipelineConfiguration pipelineConf,
      List<Issue> errors,
      Map<String, Object> runtimeParameters,
      String user,
      Map<String, ConnectionConfiguration> connections
  ) {
    int priorErrors = errors.size();
    PipelineConfigBean pipelineConfigBean = createPipelineConfigs(
        pipelineConf,
        errors,
        runtimeParameters,
        user,
        connections
    );
    return (errors.size() == priorErrors) ? pipelineConfigBean : null;
  }


  public RuleDefinitionsConfigBean createRuleDefinitionsConfigBean(
      RuleDefinitions ruleDefinitions,
      List<Issue> errors,
      Map<String, Object> runtimeParameters
  ) {
    RuleDefinitionsConfigBean ruleDefinitionsConfigBean = new RuleDefinitionsConfigBean();
    ConfigInjector.get().injectStage(
        ruleDefinitionsConfigBean,
        RULES_DEFINITION,
        getRulesConfAsStageConf(ruleDefinitions),
        runtimeParameters,
        null,
        null,
        errors
    );
    return ruleDefinitionsConfigBean;
  }

  public PipelineBean create(
      boolean forExecution,
      StageLibraryTask library,
      PipelineConfiguration pipelineConf,
      InterceptorCreatorContextBuilder interceptorContextBuilder,
      String user,
      Map<String, ConnectionConfiguration> connections,
      List<Issue> errors
  ) {
    return create(forExecution, library, pipelineConf, interceptorContextBuilder, errors, null, user, connections);
  }

  /**
   * Create PipelineBean which means instantiating all stages for the pipeline.
   *
   * For multi-threaded pipelines this method will *NOT* create all required instances since the number of required
   * instances is not known at the time of creation - for that the origin has to be initialized which is not at this
   * point. Hence this method will create only one instance of the whole pipeline and it's up to the caller to call
   * createPipelineStageBeans to instantiate remaining source-less pipelines later in the execution.
   */
  public PipelineBean create(
      boolean forExecution,
      StageLibraryTask library,
      PipelineConfiguration pipelineConf,
      InterceptorCreatorContextBuilder interceptorContextBuilder,
      List<Issue> errors,
      Map<String, Object> runtimeParameters,
      String user,
      Map<String, ConnectionConfiguration> connections
  ) {
    int priorErrors = errors.size();
    PipelineConfigBean pipelineConfigBean = create(pipelineConf, errors, runtimeParameters, user, connections);
    StageBean errorStageBean = null;
    StageBean statsStageBean = null;
    StageBean origin = null;
    PipelineStageBeans stages = null;
    PipelineStageBeans startEventBeans = null;
    PipelineStageBeans stopEventBeans = null;
    if (pipelineConfigBean != null && pipelineConfigBean.constants != null) {
      Map<String, Object> resolvedConstants = pipelineConfigBean.constants;
      if(interceptorContextBuilder != null) {
        interceptorContextBuilder
          .withExecutionMode(pipelineConfigBean.executionMode)
          .withDeliveryGuarantee(pipelineConfigBean.deliveryGuarantee)
        ;
      }

      // Instantiate usual stages
      if(!pipelineConf.getStages().isEmpty()) {
        origin = createStageBean(
            forExecution,
            library,
            pipelineConf.getStages().get(0),
            true,
            false,
            false,
            resolvedConstants,
            interceptorContextBuilder,
            user,
            connections,
            errors
        );

        stages = createPipelineStageBeans(
            forExecution,
            library,
            pipelineConf.getStages().subList(1, pipelineConf.getStages().size()),
            interceptorContextBuilder,
            resolvedConstants,
            user,
            connections,
            errors
        );
      }

      // It is not mandatory to have a stats aggregating target configured
      StageConfiguration statsStageConf = pipelineConf.getStatsAggregatorStage();
      if (statsStageConf != null) {
        statsStageBean = createStageBean(
            forExecution,
            library,
            statsStageConf,
            true,
            false,
            false,
            resolvedConstants,
            interceptorContextBuilder,
            user,
            connections,
            errors
        );
      }

      // Error stage is mandatory
      StageConfiguration errorStageConf = pipelineConf.getErrorStage();
      if (errorStageConf != null) {
        errorStageBean = createStageBean(
            forExecution,
            library,
            errorStageConf,
            true,
            true,
            false,
            resolvedConstants,
            interceptorContextBuilder,
            user,
            connections,
            errors
        );
      } else if (!(pipelineConfigBean.executionMode.equals(ExecutionMode.BATCH) ||
          pipelineConfigBean.executionMode.equals(ExecutionMode.STREAMING))) {
        errors.add(IssueCreator.getPipeline().create(
            PipelineGroups.BAD_RECORDS.name(),
            "badRecordsHandling",
            CreationError.CREATION_009
        ));
      }

      // Pipeline Lifecycle event handlers
      StageBean startBean = null;
      if(CollectionUtils.isNotEmpty(pipelineConf.getStartEventStages())) {
         startBean = createStageBean(
            forExecution,
            library,
            pipelineConf.getStartEventStages().get(0),
            true,
            false,
            true,
            resolvedConstants,
            interceptorContextBuilder,
            user,
            connections,
            errors
        );
      }
      startEventBeans = new PipelineStageBeans(startBean == null ? Collections.emptyList() : ImmutableList.of(startBean));
      StageBean stopBean = null;
      if(CollectionUtils.isNotEmpty(pipelineConf.getStopEventStages())) {
         stopBean = createStageBean(
            forExecution,
            library,
            pipelineConf.getStopEventStages().get(0),
            true,
            false,
            true,
            resolvedConstants,
            interceptorContextBuilder,
            user,
            connections,
            errors
        );
      }
      stopEventBeans = new PipelineStageBeans(stopBean == null ? Collections.emptyList() : ImmutableList.of(stopBean));

      // Validate Webhook Configs
      if (pipelineConfigBean.webhookConfigs != null && !pipelineConfigBean.webhookConfigs.isEmpty()) {
        int index = 0;
        for (PipelineWebhookConfig webhookConfig: pipelineConfigBean.webhookConfigs) {
          if (StringUtils.isEmpty(webhookConfig.webhookUrl)) {
            Issue issue = IssueCreator.getPipeline().create(
                PipelineGroups.NOTIFICATIONS.name(),
                "webhookUrl",
                CreationError.CREATION_080
            );
            issue.setAdditionalInfo("index", index);
            errors.add(issue);
            break;
          }
          index++;
        }
      }
    }

    // Something went wrong
    if(errors.size() != priorErrors) {
      return  null;
    }

    return new PipelineBean(
        pipelineConfigBean,
        origin,
        stages,
        errorStageBean,
        statsStageBean,
        startEventBeans,
        stopEventBeans
    );
  }

  private PipelineStageBeans createPipelineStageBeans(
    boolean forExecution,
    StageLibraryTask library,
    List<StageConfiguration> stageConfigurations,
    InterceptorCreatorContextBuilder interceptorContextBuilder,
    Map<String, Object> constants,
    String user,
    Map<String, ConnectionConfiguration> connections,
    List<Issue> errors
  ) {
    List<StageBean> stageBeans = new ArrayList<>(stageConfigurations.size());

    for (StageConfiguration stageConf : stageConfigurations) {
      StageBean stageBean = createStageBean(
          forExecution,
          library,
          stageConf,
          true,
          false,
          false,
          constants,
          interceptorContextBuilder,
          user,
          connections,
          errors
      );

      if (stageBean != null) {
        stageBeans.add(stageBean);
      }
    }

    return new PipelineStageBeans(stageBeans);
  }

  /**
   * Creates additional PipelineStageBeans for additional runners. Stages will share stage definition and thus
   * class loader with the first given runner. That includes stages with private class loader as well.
   *
   * @param pipelineStageBeans First runner that should be duplicated.
   * @param interceptorCreatorContextBuilder Builder for interceptor context
   * @param constants Pipeline constants
   * @param errors Any generated errors will be stored in this list
   *
   * @return PipelineStageBeans with new instances of the given stages
   */
  public PipelineStageBeans duplicatePipelineStageBeans(
    StageLibraryTask stageLib,
    PipelineStageBeans pipelineStageBeans,
    InterceptorCreatorContextBuilder interceptorCreatorContextBuilder,
    Map<String, Object> constants,
    String user,
    Map<String, ConnectionConfiguration> connections,
    List<Issue> errors
  ) {
    List<StageBean> stageBeans = new ArrayList<>(pipelineStageBeans.size());


    for(StageBean original: pipelineStageBeans.getStages()) {

      // Create StageDefinition map for this stage
      Map<Class, ServiceDefinition> services = original.getServices().stream()
        .collect(Collectors.toMap(c -> c.getDefinition().getProvides(), ServiceBean::getDefinition));

      StageBean stageBean = createStage(
          stageLib,
          original.getDefinition(),
          ClassLoaderReleaser.NOOP_RELEASER,
          original.getConfiguration(),
          services::get,
          interceptorCreatorContextBuilder,
          constants,
          user,
          connections,
          errors
      );

      if (stageBean != null) {
        stageBeans.add(stageBean);
      }
    }

    return new PipelineStageBeans(stageBeans);
  }

  public ExecutionMode getExecutionMode(PipelineFragmentConfiguration pipelineConf, List<Issue> errors) {
    ExecutionMode mode = null;
    String value = null;
    if (pipelineConf.getConfiguration("executionMode") != null) {
      if (pipelineConf.getConfiguration("executionMode").getValue() != null) {
        value = pipelineConf.getConfiguration("executionMode").getValue().toString();
      }
    }
    if (value != null) {
      try {
        mode = ExecutionMode.valueOf(value);
      } catch (IllegalArgumentException ex) {
        errors.add(IssueCreator.getPipeline().create("", "executionMode", CreationError.CREATION_070, value));
      }
    } else {
      errors.add(IssueCreator.getPipeline().create("", "executionMode", CreationError.CREATION_071));
    }
    return mode;
  }

  public SparkClusterType getClusterType(PipelineFragmentConfiguration pipelineConf, List<Issue> errors) {
    SparkClusterType clusterType = SparkClusterType.LOCAL;
    String value = null;
    if (pipelineConf.getConfiguration("clusterConfig.clusterType") != null) {
      if (pipelineConf.getConfiguration("clusterConfig.clusterType").getValue() != null) {
        value = pipelineConf.getConfiguration("clusterConfig.clusterType").getValue().toString();
      }
    }
    if (value != null) {
      try {
        clusterType = SparkClusterType.valueOf(value);
      } catch (IllegalArgumentException ex) {
        errors.add(IssueCreator.getPipeline().create(
            PipelineGroups.CLUSTER.name(),
            "clusterConfig.clusterType",
            CreationError.CREATION_1000,
            value
        ));
      }
    }
    return clusterType;
  }

  public String getMesosDispatcherURL(PipelineConfiguration pipelineConf) {
    String value = null;
    if (pipelineConf.getConfiguration("mesosDispatcherURL") != null) {
      value = pipelineConf.getConfiguration("mesosDispatcherURL").getValue().toString();
    }
    return value;
  }

  public String getHdfsS3ConfDirectory(PipelineConfiguration pipelineConf) {
    String value = null;
    if (pipelineConf.getConfiguration("hdfsS3ConfDir") != null) {
      value = pipelineConf.getConfiguration("hdfsS3ConfDir").getValue().toString();
    }
    return value;
  }

  /**
   * Create new instance of StageBean.
   *
   * This method can be used outside of pipeline context (for example for detached stage).
   *
   * @param forExecution If the instance is going to be used for execution (and for example private classloaders must be respected)
   * @param library Stage Library from which a definition will be loaded
   * @param stageConf Stage configuration
   * @param errorStage True if the stage needs to be declared as error stage
   * @param pipelineLifecycleStage True if the stage needs to be declared as pipeline lifecycle stage
   * @param constants Pipeline constants (runtime parameters)
   * @param user The user who is creating the pipeline
   * @param connections A map of Connections
   * @param errors List where all errors will be persisted
   * @return New StageBean instance or null on any error
   */
  public StageBean createStageBean(
      boolean forExecution,
      StageLibraryTask library,
      StageConfiguration stageConf,
      boolean validateAnnotations,
      boolean errorStage,
      boolean pipelineLifecycleStage,
      Map<String, Object> constants,
      InterceptorCreatorContextBuilder interceptorContextBuilder,
      String user,
      Map<String, ConnectionConfiguration> connections,
      List<Issue> errors
  ) {
    IssueCreator issueCreator = IssueCreator.getStage(stageConf.getInstanceName());
    StageBean bean = null;
    StageDefinition stageDef = library.getStage(stageConf.getLibrary(), stageConf.getStageName(),
                                                forExecution);
    if (stageDef != null) {
      // Pipeline lifecycle events validation must match, whether it's also marked as error stage does not matter
      if(validateAnnotations) {
        if (pipelineLifecycleStage) {
          if (!stageDef.isPipelineLifecycleStage()) {
            errors.add(issueCreator.create(
              CreationError.CREATION_018,
              stageDef.getLibraryLabel(),
              stageDef.getLabel(),
              stageConf.getStageVersion())
            );
          }
          // For non pipeline lifecycle stages, the error stage annotation must match
        } else if (stageDef.isErrorStage() != errorStage) {
          if (stageDef.isErrorStage()) {
            errors.add(issueCreator.create(CreationError.CREATION_007, stageDef.getLibraryLabel(), stageDef.getLabel(),
              stageConf.getStageVersion()));
          } else {
            errors.add(issueCreator.create(CreationError.CREATION_008, stageDef.getLibraryLabel(), stageDef.getLabel(),
              stageConf.getStageVersion()));
          }
        }
      }

      bean = createStage(
        library,
        stageDef,
        library,
        stageConf,
        serviceClass -> library.getServiceDefinition(serviceClass, true),
        interceptorContextBuilder,
        constants,
        user,
        connections,
        errors
      );
    } else {
      if(library.getLegacyStageLibs().contains(stageConf.getLibrary())) {
        errors.add(issueCreator.create(
          CreationError.CREATION_072,
          stageConf.getLibrary()
        ));
      } else {
        errors.add(issueCreator.create(
          CreationError.CREATION_006,
          stageConf.getLibrary(),
          stageConf.getStageName(),
          stageConf.getStageVersion()
        ));
      }
    }
    return bean;
  }

  @SuppressWarnings("unchecked")
  public static StageConfiguration getPipelineConfAsStageConf(PipelineFragmentConfiguration pipelineConf) {
    return new StageConfiguration(
        null,
        "none",
        "pipeline",
        pipelineConf.getVersion(),
        pipelineConf.getConfiguration(),
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList()
    );
  }


  @SuppressWarnings("unchecked")
  public static StageConfiguration getRulesConfAsStageConf(RuleDefinitions ruleDefinitions) {
    return new StageConfiguration(
        null,
        "none",
        "pipeline",
        ruleDefinitions.getVersion(),
        ruleDefinitions.getConfiguration() != null ? ruleDefinitions.getConfiguration() : Collections.emptyList(),
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList()
    );
  }

  @SuppressWarnings("unchecked")
  private PipelineConfigBean createPipelineConfigs(
      PipelineConfiguration pipelineConf,
      List<Issue> errors,
      Map<String, Object> runtimeParameters,
      String user,
      Map<String, ConnectionConfiguration> connections
  ) {
    PipelineConfigBean pipelineConfigBean = new PipelineConfigBean();
    StageConfiguration stageConf = getPipelineConfAsStageConf(pipelineConf);
    ConfigInjector.StageInjectorContext context = new ConfigInjector.StageInjectorContext(PIPELINE_DEFINITION,
        stageConf, runtimeParameters, user, connections, errors);
    if (ConfigInjector.get().createConfigBeans(
        pipelineConfigBean,
        "",
        context
    )) {

      // To support parameters in Pipeline Configuration inject "parameters" (constants) field first
      Config parametersConfigConf = stageConf.getConfig(PARAMETERS);
      ConfigDefinition parametersConfigDef = PIPELINE_DEFINITION.getConfigDefinitionsMap().get(PARAMETERS);
      if (parametersConfigConf != null) {
        try {
          ConfigInjector.get().injectConfigValue(
              pipelineConfigBean,
              PipelineConfigBean.class.getField(PARAMETERS),
              parametersConfigConf.getValue(),
              parametersConfigDef,
              new ConfigInjector.StageInjectorContext(context, Collections.EMPTY_MAP)
          );
        } catch (NoSuchFieldException ex) {
          IssueCreator issueCreator = IssueCreator.getStage(stageConf.getStageName());
          errors.add(issueCreator.create(CreationError.CREATION_000, "pipeline", parametersConfigDef.getLabel(), ex.toString()));
          pipelineConfigBean.constants = Collections.EMPTY_MAP;
        }
      }

      Map<String, Object> resolvedConstants = pipelineConfigBean.constants;

      if (pipelineConfigBean.constants == null) {
        pipelineConfigBean.constants = Collections.emptyMap();
        resolvedConstants = Collections.emptyMap();
      } else {
        // Merge constant and runtime Constants
        if (runtimeParameters != null) {
          for (String key: runtimeParameters.keySet()) {
            if (resolvedConstants.containsKey(key)) {
              resolvedConstants.put(key, runtimeParameters.get(key));
            }
          }
        }
      }

      ConfigInjector.get().injectConfigs(
          pipelineConfigBean,
          "",
          new ConfigInjector.StageInjectorContext(context, pipelineConfigBean.constants)
      );

      if (runtimeParameters != null) {
        pipelineConfigBean.constants = resolvedConstants;
      }
    }
    return pipelineConfigBean;
  }

  StageBean createStage(
      StageLibraryTask stageLib,
      StageDefinition stageDef,
      ClassLoaderReleaser classLoaderReleaser,
      StageConfiguration stageConf,
      Function<Class, ServiceDefinition> serviceDefinitionResolver,
      InterceptorCreatorContextBuilder interceptorContextBuilder,
      Map<String, Object> pipelineConstants,
      String user,
      Map<String, ConnectionConfiguration> connections,
      List<Issue> errors
  ) {
    Stage stage;
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(stageDef.getStageClassLoader());
      stage = createStageInstance(stageDef, stageConf.getInstanceName(), errors);
      if (stage != null) {
        ConfigInjector.get().injectStage(stage,stageDef, stageConf, pipelineConstants, user, connections, errors);
      }
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }
    StageConfigBean stageConfigBean = new StageConfigBean();
    ConfigInjector.get().injectStage(stageConfigBean, stageDef, stageConf, pipelineConstants, user, connections, errors);

    // Create services
    List<ServiceBean> services = new ArrayList<>();
    for(ServiceConfiguration serviceConf : stageConf.getServices()) {
      ServiceDefinition serviceDef = serviceDefinitionResolver.apply(serviceConf.getService());

      ServiceBean serviceBean = createService(
        stageConf.getInstanceName(),
        serviceDef,
        classLoaderReleaser,
        serviceConf,
        pipelineConstants,
        user,
        connections,
        errors
      );

      if(serviceBean != null) {
        services.add(serviceBean);
      }
    }

    if(!errors.isEmpty()) {
      return null;
    }

    return new StageBean(
      stageDef,
      stageConf,
      stageConfigBean,
      stage,
      classLoaderReleaser,
      services,
      createInterceptors(stageLib, stageConf, stageDef, interceptorContextBuilder, InterceptorCreator.InterceptorType.PRE_STAGE, errors),
      createInterceptors(stageLib, stageConf, stageDef, interceptorContextBuilder, InterceptorCreator.InterceptorType.POST_STAGE, errors)
    );
  }

  private Stage createStageInstance(StageDefinition stageDef, String stageName, List<Issue> errors) {
    Stage stage = null;
    try {
      stage = stageDef.getStageClass().newInstance();
    } catch (InstantiationException | IllegalAccessException ex) {
      IssueCreator issueCreator = IssueCreator.getStage(stageName);
      errors.add(issueCreator.create(CreationError.CREATION_000, "stage", stageDef.getLabel(), ex.toString()));
    }
    return stage;
  }

  ServiceBean createService(
      String stageName,
      ServiceDefinition serviceDef,
      ClassLoaderReleaser classLoaderReleaser,
      ServiceConfiguration serviceConf,
      Map<String, Object> pipelineConstants,
      String user,
      Map<String, ConnectionConfiguration> connections,
      List<Issue> errors
  ) {
    Utils.checkNotNull(serviceDef, "ServiceDefinition for " + serviceConf.getService().getName());
    Service service;
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(serviceDef.getStageClassLoader());
      service = createServiceInstance(stageName, serviceDef, errors);
      if (service != null) {
        ConfigInjector.get().injectService(
            service,
            stageName,
            serviceDef,
            serviceConf,
            pipelineConstants,
            user,
            connections,
            errors
        );
      }
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }
    return (errors.isEmpty()) ? new ServiceBean(serviceDef, serviceConf, service, classLoaderReleaser) : null;
  }

  private Service createServiceInstance(String stageName, ServiceDefinition serviceDef, List<Issue> errors) {
    Service service = null;
    try {
      service = serviceDef.getKlass().newInstance();
    } catch (InstantiationException | IllegalAccessException ex) {
      IssueCreator issueCreator = IssueCreator.getService(stageName, serviceDef.getKlass().getName());
      errors.add(issueCreator.create(CreationError.CREATION_000, "service", serviceDef.getKlass().getName(), ex.toString()));
    }
    return service;
  }

  /**
   * Create interceptors for given stage.
   */
  public List<InterceptorBean> createInterceptors(
    StageLibraryTask stageLib,
    StageConfiguration stageConfiguration,
    StageDefinition stageDefinition,
    InterceptorCreatorContextBuilder contextBuilder,
    InterceptorCreator.InterceptorType interceptorType,
    List<Issue> issues
  ) {
    List<InterceptorBean> beans = new ArrayList<>();
    if(contextBuilder == null) {
      return beans;
    }

    for(InterceptorDefinition definition : stageLib.getInterceptorDefinitions()) {
      InterceptorBean bean = createInterceptor(stageLib, definition, stageConfiguration, stageDefinition, contextBuilder, interceptorType, issues);
      if (bean != null) {
        beans.add(bean);
      }
    }

    return beans;
  }

  /**
   * Create a default interceptor for given InterceptorDefinition. This method might
   * return null as the underlying interface for default creation allows it as well -
   * in such case no interceptor is needed.
   */
  public InterceptorBean createInterceptor(
    StageLibraryTask stageLib,
    InterceptorDefinition definition,
    StageConfiguration stageConfiguration,
    StageDefinition stageDefinition,
    InterceptorCreatorContextBuilder contextBuilder,
    InterceptorCreator.InterceptorType interceptorType,
    List<Issue> issues
  ) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    InterceptorCreator.Context context = contextBuilder.buildFor(
      definition.getLibraryDefinition().getName(),
      definition.getKlass().getName(),
      stageConfiguration,
      stageDefinition,
      interceptorType
    );

    try {
      Thread.currentThread().setContextClassLoader(definition.getStageClassLoader());
      InterceptorCreator creator = definition.getDefaultCreator().newInstance();
      Interceptor interceptor = creator.create(context);

      if(interceptor == null) {
        return null;
      }

      return new InterceptorBean(
        definition,
        interceptor,
        stageLib
      );
    } catch (IllegalAccessException|InstantiationException e) {
      LOG.debug("Can't instantiate interceptor: {}", e.toString(), e);
      IssueCreator issueCreator = IssueCreator.getStage(stageDefinition.getName());
      issues.add(issueCreator.create(
        CreationError.CREATION_000, "interceptor", definition.getKlass().getName(), e.toString()
      ));
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }

    return null;
  }

}
