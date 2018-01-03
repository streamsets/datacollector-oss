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
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineGroups;
import com.streamsets.datacollector.config.PipelineWebhookConfig;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.ServiceConfiguration;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.definition.StageDefinitionExtractor;
import com.streamsets.datacollector.stagelibrary.ClassLoaderReleaser;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.IssueCreator;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.Service;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class PipelineBeanCreator {
  public static final String PIPELINE_LIB_DEFINITION = "Pipeline";
  private static final String RULE_DEFINITIONS_LIB_DEFINITION = "RuleDefinitions";
  private static final String PARAMETERS = "constants";

  private static final PipelineBeanCreator CREATOR = new PipelineBeanCreator() {
  };

  public static PipelineBeanCreator get() {
    return CREATOR;
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
    return StageDefinitionExtractor.get().extract(libraryDef, PipelineConfigBean.class, "Pipeline Config Definitions");
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

  public PipelineConfigBean create(
      PipelineConfiguration pipelineConf,
      List<Issue> errors,
      Map<String, Object> runtimeParameters
  ) {
    int priorErrors = errors.size();
    PipelineConfigBean pipelineConfigBean = createPipelineConfigs(pipelineConf, errors, runtimeParameters);
    return (errors.size() == priorErrors) ? pipelineConfigBean : null;
  }


  public RuleDefinitionsConfigBean createRuleDefinitionsConfigBean(
      RuleDefinitions ruleDefinitions,
      List<Issue> errors,
      Map<String, Object> runtimeParameters
  ) {
    RuleDefinitionsConfigBean ruleDefinitionsConfigBean = new RuleDefinitionsConfigBean();
    ConfigInjector.get().injectStage(ruleDefinitionsConfigBean, RULES_DEFINITION, getRulesConfAsStageConf(ruleDefinitions), runtimeParameters, errors);
    return ruleDefinitionsConfigBean;
  }

  public PipelineBean create(
      boolean forExecution,
      StageLibraryTask library,
      PipelineConfiguration pipelineConf,
      List<Issue> errors
  ) {
    return create(forExecution, library, pipelineConf, errors, null);
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
      List<Issue> errors,
      Map<String, Object> runtimeParameters
  ) {
    int priorErrors = errors.size();
    PipelineConfigBean pipelineConfigBean = create(pipelineConf, errors, runtimeParameters);
    StageBean errorStageBean = null;
    StageBean statsStageBean = null;
    StageBean origin = null;
    PipelineStageBeans stages = null;
    PipelineStageBeans startEventBeans = null;
    PipelineStageBeans stopEventBeans = null;
    if (pipelineConfigBean != null && pipelineConfigBean.constants != null) {

      Map<String, Object> resolvedConstants = pipelineConfigBean.constants;

      // Instantiate usual stages
      if(!pipelineConf.getStages().isEmpty()) {
        origin = createStageBean(
            forExecution,
            library,
            pipelineConf.getStages().get(0),
            false,
            false,
            resolvedConstants,
            errors
        );

        stages = createPipelineStageBeans(
            forExecution,
            library,
            pipelineConf.getStages().subList(1, pipelineConf.getStages().size()),
            resolvedConstants,
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
            false,
            false,
            resolvedConstants,
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
            false,
            resolvedConstants,
            errors
        );
      } else {
        errors.add(IssueCreator.getPipeline().create(
            PipelineGroups.BAD_RECORDS.name(),
            "badRecordsHandling",
            CreationError.CREATION_009
        ));
      }

      // Pipeline Lifecycle event handlers
      StageBean startBean = null;
      if(!pipelineConf.getStartEventStages().isEmpty()) {
         startBean = createStageBean(
            forExecution,
            library,
            pipelineConf.getStartEventStages().get(0),
            false,
            true,
            resolvedConstants,
            errors
        );
      }
      startEventBeans = new PipelineStageBeans(startBean == null ? Collections.emptyList() : ImmutableList.of(startBean));
      StageBean stopBean = null;
      if(!pipelineConf.getStopEventStages().isEmpty()) {
         stopBean = createStageBean(
            forExecution,
            library,
            pipelineConf.getStopEventStages().get(0),
            false,
            true,
            resolvedConstants,
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
    Map<String, Object> constants,
    List<Issue> errors
  ) {
    List<StageBean> stageBeans = new ArrayList<>(stageConfigurations.size());

    for (StageConfiguration stageConf : stageConfigurations) {
      StageBean stageBean = createStageBean(
          forExecution,
          library,
          stageConf,
          false,
          false,
          constants,
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
   * @param constants Pipeline constants
   * @param errors Any generated errors will be stored in this list
   *
   * @return PipelineStageBeans with new instances of the given stages
   */
  public PipelineStageBeans duplicatePipelineStageBeans(
    PipelineStageBeans pipelineStageBeans,
    Map<String, Object> constants,
    List<Issue> errors
  ) {
    List<StageBean> stageBeans = new ArrayList<>(pipelineStageBeans.size());


    for(StageBean original: pipelineStageBeans.getStages()) {

      // Create StageDefinition map for this stage
      Map<Class, ServiceDefinition> services = original.getServices().stream()
        .collect(Collectors.toMap(c -> c.getDefinition().getKlass(), ServiceBean::getDefinition));

      StageBean stageBean = createStage(
          original.getDefinition(),
          ClassLoaderReleaser.NOOP_RELEASER,
          original.getConfiguration(),
          services::get,
          constants,
          errors
      );

      if (stageBean != null) {
        stageBeans.add(stageBean);
      }
    }

    return new PipelineStageBeans(stageBeans);
  }

  public ExecutionMode getExecutionMode(PipelineConfiguration pipelineConf, List<Issue> errors) {
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

  private StageBean createStageBean(
      boolean forExecution,
      StageLibraryTask library,
      StageConfiguration stageConf,
      boolean errorStage,
      boolean pipelineLifecycleStage,
      Map<String, Object> constants,
      List<Issue> errors
  ) {
    IssueCreator issueCreator = IssueCreator.getStage(stageConf.getInstanceName());
    StageBean bean = null;
    StageDefinition stageDef = library.getStage(stageConf.getLibrary(), stageConf.getStageName(),
                                                forExecution);
    if (stageDef != null) {
      // Pipeline lifecycle events validation must match, whether it's also marked as error stage does not matter
      if(pipelineLifecycleStage) {
        if(!stageDef.isPipelineLifecycleStage()) {
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

      bean = createStage(
        stageDef,
        library,
        stageConf,
        serviceClass -> library.getServiceDefinition(serviceClass, true),
        constants,
        errors
      );
    } else {
      errors.add(issueCreator.create(CreationError.CREATION_006, stageConf.getLibrary(), stageConf.getStageName(),
                                     stageConf.getStageVersion()));
    }
    return bean;
  }

  @SuppressWarnings("unchecked")
  public static StageConfiguration getPipelineConfAsStageConf(PipelineConfiguration pipelineConf) {
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
      Map<String, Object> runtimeParameters
  ) {
    PipelineConfigBean pipelineConfigBean = new PipelineConfigBean();
    StageConfiguration stageConf = getPipelineConfAsStageConf(pipelineConf);
    if (ConfigInjector.get().createConfigBeans(pipelineConfigBean, "", new ConfigInjector.StageInjectorContext(PIPELINE_DEFINITION, stageConf, runtimeParameters, errors))) {

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
              new ConfigInjector.StageInjectorContext(PIPELINE_DEFINITION, stageConf, Collections.EMPTY_MAP, errors)
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

      ConfigInjector.get().injectConfigs(pipelineConfigBean, "", new ConfigInjector.StageInjectorContext(PIPELINE_DEFINITION, stageConf, pipelineConfigBean.constants, errors));

      if (runtimeParameters != null) {
        pipelineConfigBean.constants = resolvedConstants;
      }
    }
    return pipelineConfigBean;
  }

  StageBean createStage(
      StageDefinition stageDef,
      ClassLoaderReleaser classLoaderReleaser,
      StageConfiguration stageConf,
      Function<Class, ServiceDefinition> serviceDefinitionResolver,
      Map<String, Object> pipelineConstants,
      List<Issue> errors
  ) {
    Stage stage;
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(stageDef.getStageClassLoader());
      stage = createStageInstance(stageDef, stageConf.getInstanceName(), errors);
      if (stage != null) {
        ConfigInjector.get().injectStage(stage,stageDef, stageConf, pipelineConstants, errors);
      }
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }
    StageConfigBean stageConfigBean = new StageConfigBean();
    ConfigInjector.get().injectStage(stageConfigBean, stageDef, stageConf, pipelineConstants, errors);

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
        errors
      );

      if(serviceBean != null) {
        services.add(serviceBean);
      }
    }

    return (errors.isEmpty()) ? new StageBean(stageDef, stageConf, stageConfigBean, stage, classLoaderReleaser, services) : null;
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
      List<Issue> errors
  ) {
    Utils.checkNotNull(serviceDef, "ServiceDefinition can't be null.");
    Service service;
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(serviceDef.getStageClassLoader());
      service = createServiceInstance(stageName, serviceDef, errors);
      if (service != null) {
        ConfigInjector.get().injectService(service, stageName, serviceDef, serviceConf, pipelineConstants, errors);
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

}
