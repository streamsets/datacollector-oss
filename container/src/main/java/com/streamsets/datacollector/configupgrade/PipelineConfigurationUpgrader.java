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
package com.streamsets.datacollector.configupgrade;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.ServiceConfiguration;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.creation.PipelineConfigUpgrader;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineConfigurationUtil;
import com.streamsets.datacollector.util.Version;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.IssueCreator;
import com.streamsets.datacollector.validation.PipelineConfigurationValidator;
import com.streamsets.datacollector.validation.ValidationError;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipelineConfigurationUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineConfigurationUpgrader.class);

  private static final PipelineConfigurationUpgrader UPGRADER = new PipelineConfigurationUpgrader() {
  };

  public static PipelineConfigurationUpgrader get() {
    return UPGRADER;
  }

  protected PipelineConfigurationUpgrader() {
  }

  /**
   * Upgrade whole pipeline at once and return updated variant.
   *
   * @param library Stage Library
   * @param pipelineConf Existing Pipeline Configuration
   * @param issues Issues
   * @return Upgraded pipeline configuration or null on any error
   */
  public PipelineConfiguration upgradeIfNecessary(
      StageLibraryTask library,
      BuildInfo buildInfo,
      PipelineConfiguration pipelineConf,
      List<Issue> issues
  ) {
    Preconditions.checkArgument(issues.isEmpty(), "Given list of issues must be empty.");

    // Check that this pipeline wasn't created by a higher version
    // SDC version was only added in 3.7.0 to the pipeline bean, so for really ancient pipelines, this field wouldn't
    // exists. But that is actually fine for purpose of this check - those old pipelines are always created on SDC
    // version lower then this one.
    if(!StringUtils.isEmpty(pipelineConf.getInfo().getSdcVersion())) {
      Version createdVersion = new Version(pipelineConf.getInfo().getSdcVersion());
      if (createdVersion.isGreaterThan(buildInfo.getVersion())) {
        LOG.error("Validation failed: {} vs {}", pipelineConf.getInfo().getSdcVersion(), buildInfo.getVersion());
        IssueCreator issueCreator = IssueCreator.getPipeline();
        issues.add(issueCreator.create(
            ValidationError.VALIDATION_0096,
            pipelineConf.getInfo().getSdcVersion(),
            buildInfo.getVersion()
        ));

        // In case that the version is higher then the one we can work with, we don't upgrade the rest (too many
        // unrelated errors would be displayed in that case).
        return null;
      }
    }

    boolean upgrade;
    // Firstly upgrading schema if needed, then data
    upgrade = needsSchemaUpgrade(pipelineConf, issues);
    if(upgrade && issues.isEmpty()) {
      pipelineConf = upgradeSchema(library, pipelineConf, issues);
    }

    // Something went wrong with the schema upgrade
    if(!issues.isEmpty()) {
      return null;
    }

    // Upgrading data if needed
    upgrade = needsUpgrade(library, pipelineConf, issues);
    if (upgrade && issues.isEmpty()) {
      // we try to upgrade only if we have all defs for the pipelineConf
      pipelineConf = upgrade(library, pipelineConf, issues);
    }

    // And lastly update the SDC version (since the pipeline was just changed)
    if(issues.isEmpty()) {
      pipelineConf.getInfo().setSdcVersion(buildInfo.getVersion());
    }

    return (issues.isEmpty()) ? pipelineConf : null;
  }

  /**
   * Upgrade detached stage (stage not associated directly with the pipeline).
   *
   * @param libraryTask Stage Library
   * @param stageConf Existing stage configuration
   * @param issues Issues
   * @return Upgraded stage configuration or null on any error
   */
  public StageConfiguration upgradeIfNecessary(StageLibraryTask libraryTask, StageConfiguration stageConf, List<Issue> issues) {
    Preconditions.checkArgument(issues.isEmpty(), "Given list of issues must be empty.");
    boolean upgrade = needsUpgrade(libraryTask, stageConf, issues);

    if(upgrade) {
      stageConf = upgradeIfNeeded(libraryTask, stageConf, issues);
    }

    return issues.isEmpty() ? stageConf : null;
  }

  private boolean needsSchemaUpgrade(PipelineConfiguration pipelineConf, List<Issue> ownIssues) {
    return pipelineConf.getSchemaVersion() != PipelineStoreTask.SCHEMA_VERSION;
  }

  private PipelineConfiguration upgradeSchema(
      StageLibraryTask library,
      PipelineConfiguration pipelineConf,
      List<Issue> issues
  ) {
    LOG.debug("Upgrading schema from version {} on pipeline {}", pipelineConf.getSchemaVersion(), pipelineConf.getUuid());
    switch (pipelineConf.getSchemaVersion()) {
      case 1:
        upgradeSchema1to2(pipelineConf, issues);
        // fall through
      case 2:
        upgradeSchema2to3(pipelineConf, issues);
        // fall through
      case 3:
        upgradeSchema3to4(pipelineConf, issues);
        // fall through
      case 4:
        upgradeSchema4to5(pipelineConf, issues);
        // fall through
      case 5:
        upgradeSchema5to6(library, pipelineConf, issues);
        break;
      default:
        issues.add(IssueCreator.getPipeline().create(ValidationError.VALIDATION_0000, pipelineConf.getSchemaVersion()));
    }

    pipelineConf.setSchemaVersion(PipelineStoreTask.SCHEMA_VERSION);
    return issues.isEmpty() ? pipelineConf : null;
  }

  private void upgradeSchema1to2(PipelineConfiguration pipelineConf, List<Issue> issues) {
    // Version 1 did not have any eventLanes configuration so this will be null and we need to convert it
    // to empty list instead for all stages.

    // Normal stages
    for(StageConfiguration stage: pipelineConf.getStages()) {
      convertEventLaneNullToEmptyList(stage);
    }

    // Extra stages
    convertEventLaneNullToEmptyList(pipelineConf.getErrorStage());
    convertEventLaneNullToEmptyList(pipelineConf.getStatsAggregatorStage());
  }

  private void upgradeSchema2to3(PipelineConfiguration pipelineConf, List<Issue> issues) {
    // Added new attribute "pipelineId"
    if (pipelineConf.getPipelineId() == null) {
      pipelineConf.setPipelineId(pipelineConf.getInfo().getPipelineId());
    }
  }

  private void upgradeSchema3to4(PipelineConfiguration pipelineConf, List<Issue> issues) {
    // Added new attributes:
    // * statEventStages
    // * stopEventStages
    if(pipelineConf.getStartEventStages() == null) {
      pipelineConf.setStartEventStages(Collections.emptyList());
    }
    if(pipelineConf.getStopEventStages() == null) {
      pipelineConf.setStopEventStages(Collections.emptyList());
    }
  }

  private void upgradeSchema4to5(PipelineConfiguration pipelineConf, List<Issue> issues) {
    // Each stage have now a new field called "services" that we will by default fill with empty array

    // Normal stages
    for(StageConfiguration stage: pipelineConf.getStages()) {
      convertNullServiceToEmptyList(stage);
    }

    // Start event stages
    for(StageConfiguration stage : pipelineConf.getStartEventStages()) {
      convertNullServiceToEmptyList(stage);
    }

    // Stop event stages
    for(StageConfiguration stage : pipelineConf.getStopEventStages()) {
      convertNullServiceToEmptyList(stage);
    }
    // Extra stages
    convertNullServiceToEmptyList(pipelineConf.getErrorStage());
    convertNullServiceToEmptyList(pipelineConf.getStatsAggregatorStage());

    //Fix: Stats aggregator stage upgrade if needed
    boolean isPipelineClusterMode = PipelineConfigUpgrader.isPipelineClusterMode(pipelineConf.getConfiguration());
    if (isPipelineClusterMode && pipelineConf.getStatsAggregatorStage() != null) {
      final String statsAggregatorStageNameFromStageConfig = PipelineConfigurationValidator.getStageDefQualifiedName(
          pipelineConf.getStatsAggregatorStage().getLibrary(),
          pipelineConf.getStatsAggregatorStage().getStageName(),
          String.valueOf(pipelineConf.getStatsAggregatorStage().getStageVersion())
      );
      if(statsAggregatorStageNameFromStageConfig.equals(PipelineConfigBean.STATS_DPM_DIRECTLY_TARGET)) {
        String[] stageQualfiedParts = PipelineConfigurationValidator
            .getSpecialStageDefQualifiedNameParts(PipelineConfigBean.STATS_AGGREGATOR_DEFAULT);
        Utils.checkArgument(
            stageQualfiedParts.length == 3,
            Utils.format(
                "Wrong Stage Qualified Name {}, does not contain 3 parts",
                PipelineConfigBean.STATS_AGGREGATOR_DEFAULT
            )
        );
        String library = stageQualfiedParts[0];
        String stageName = stageQualfiedParts[1];
        int stageVersion = Integer.parseInt(stageQualfiedParts[2]);

        //Reset the Stats aggregator to Discard
        pipelineConf.setStatsAggregatorStage(
            new StageConfiguration(
                "Discard_StatsAggregatorStage",
                library,
                stageName,
                stageVersion,
                Collections.emptyList(),
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList()
            )
        );
      }
    }
  }

  private void upgradeSchema5to6(StageLibraryTask library, PipelineConfiguration pipelineConf, List<Issue> issues) {
    // Added new attributes:
    // * testOriginStage
    if (pipelineConf.getErrorStage() == null) {
      StageConfiguration testOriginStageInstance = PipelineConfigurationUtil.getStageConfigurationWithDefaultValues(
          library,
          PipelineConfigBean.DEFAULT_TEST_ORIGIN_LIBRARY_NAME,
          PipelineConfigBean.DEFAULT_TEST_ORIGIN_STAGE_NAME,
          PipelineConfigBean.DEFAULT_TEST_ORIGIN_STAGE_NAME + "_TestOriginStage",
          "Test Origin - "
      );
      if (testOriginStageInstance != null) {
        testOriginStageInstance.setOutputLanes(
            ImmutableList.of(testOriginStageInstance.getInstanceName() + "OutputLane1")
        );
        pipelineConf.setTestOriginStage(testOriginStageInstance);
      }
    }
  }

  private void convertNullServiceToEmptyList(StageConfiguration stage) {
    if(stage != null && stage.getServices() == null) {
      stage.setServices(Collections.emptyList());
    }
  }

  private void convertEventLaneNullToEmptyList(StageConfiguration stage) {
    if(stage != null && stage.getEventLanes() == null) {
      stage.setEventLanes(Collections.<String>emptyList());
    }
  }

  public StageDefinition getPipelineDefinition() {
    return PipelineBeanCreator.PIPELINE_DEFINITION;
  }

  boolean needsUpgrade(StageLibraryTask library, PipelineConfiguration pipelineConf, List<Issue> issues) {
    boolean upgrade;

    // pipeline confs
    StageConfiguration pipelineConfs = PipelineBeanCreator.getPipelineConfAsStageConf(pipelineConf);
    upgrade = needsUpgrade(library, getPipelineDefinition(), pipelineConfs, issues);

    // pipeline aggregating sink stage confs
    StageConfiguration statsAggTargetConf = pipelineConf.getStatsAggregatorStage();
    if (statsAggTargetConf != null) {
      upgrade |= needsUpgrade(library, statsAggTargetConf, issues);
    }

    // pipeline error stage confs
    StageConfiguration errorStageConf = pipelineConf.getErrorStage();
    if (errorStageConf != null) {
      upgrade |= needsUpgrade(library, errorStageConf, issues);
    }

    // Pipeline start and stop events
    if(pipelineConf.getStartEventStages() != null) {
      for(StageConfiguration conf : pipelineConf.getStartEventStages()) {
        upgrade |= needsUpgrade(library, conf, issues);
      }
    }
    if(pipelineConf.getStopEventStages() != null) {
      for(StageConfiguration conf : pipelineConf.getStopEventStages()) {
        upgrade |= needsUpgrade(library, conf, issues);
      }
    }

    // Test origin
    if(pipelineConf.getTestOriginStage() != null) {
      upgrade |= needsUpgrade(library, pipelineConf.getTestOriginStage(), issues);
    }

    // pipeline stages confs
    for (StageConfiguration conf : pipelineConf.getStages()) {
      upgrade |= needsUpgrade(library, conf, issues);
    }
    return upgrade;
  }

  static boolean needsUpgrade(StageLibraryTask library, StageConfiguration conf, List<Issue> issues) {
    StageDefinition def = library.getStage(conf.getLibrary(), conf.getStageName(), false);
    return needsUpgrade(library, def, conf, issues);
  }

  static boolean needsUpgrade(StageLibraryTask library, StageDefinition def, StageConfiguration conf, List<Issue> issues) {
    boolean upgrade = false;
    if (def == null) {
      if(library.getLegacyStageLibs().contains(conf.getLibrary())) {
        issues.add(IssueCreator.getStage(conf.getInstanceName()).create(
          ContainerError.CONTAINER_0905,
          conf.getLibrary()
        ));
      } else {
        issues.add(IssueCreator.getStage(conf.getInstanceName()).create(
          ContainerError.CONTAINER_0901,
          conf.getLibrary(),
          conf.getStageName()
        ));
      }
    } else {
      // Go over services first
      for(ServiceConfiguration serviceConf: conf.getServices()) {
        ServiceDefinition serviceDef = library.getServiceDefinition(serviceConf.getService(), false);
        if(serviceDef == null) {
          issues.add(IssueCreator.getStage(conf.getInstanceName()).create(
              ContainerError.CONTAINER_0903,
              conf.getLibrary(),
              conf.getStageName()
          ));
        } else {
          upgrade |= needsUpgrade(
              serviceDef.getVersion(),
              serviceConf.getServiceVersion(),
              IssueCreator.getService(conf.getInstanceName(), serviceConf.getService().getName()),
              issues
          );
        }
      }

      // Validate version of the stage itself
      upgrade |= needsUpgrade(
          def.getVersion(),
          conf.getStageVersion(),
          IssueCreator.getStage(conf.getInstanceName()),
          issues
      );
    }
    return upgrade;
  }

  static boolean needsUpgrade(int defVersion, int currentVersion, IssueCreator issueCreator, List<Issue> issues) {
    boolean upgrade = false;
    int versionDiff = defVersion - currentVersion;
    versionDiff = Integer.compare(versionDiff, 0);
    switch (versionDiff) {
      case 0: // no change
        break;
      case 1: // current def is newer
        upgrade = true;
        break;
      case -1: // current def is older
        issues.add(issueCreator.create(
            ContainerError.CONTAINER_0902,
            currentVersion,
            defVersion
        ));
        break;
      default:
        throw new IllegalStateException("Unexpected version diff " + versionDiff);
    }
    return upgrade;
  }

  PipelineConfiguration upgrade(StageLibraryTask library, PipelineConfiguration pipelineConf, List<Issue> issues) {
    List<Issue> ownIssues = new ArrayList<>();

    // upgrade pipeline level configs if necessary
    StageConfiguration pipelineConfs = PipelineBeanCreator.getPipelineConfAsStageConf(pipelineConf);
    if (needsUpgrade(library, getPipelineDefinition(), pipelineConfs, issues)) {
      String sourceName = null;
      for (StageConfiguration stageConf: pipelineConf.getStages()) {
        if (stageConf.getInputLanes().isEmpty()) {
          sourceName = stageConf.getStageName();
        }
      }
      List<Config> configList = pipelineConfs.getConfiguration();
      // config 'sourceName' used by upgrader v3 to v4
      configList.add(new Config("sourceName", sourceName));
      pipelineConfs.setConfig(configList);
      pipelineConfs = upgradeIfNeeded(library, getPipelineDefinition(), pipelineConfs, issues);
      configList = pipelineConfs.getConfiguration();
      int index = -1;
      for (int i = 0; i < configList.size(); i++) {
        Config config = configList.get(i);
        if (config.getName().equals("sourceName")) {
          index = i;
          break;
        }
      }
      configList.remove(index);
      pipelineConfs.setConfig(configList);
    }
    List<StageConfiguration> stageConfs = new ArrayList<>();


    // upgrade aggregating stage if present and if necessary
    StageConfiguration statsAggregatorStageConf = pipelineConf.getStatsAggregatorStage();
    if (statsAggregatorStageConf != null) {
      statsAggregatorStageConf = upgradeIfNeeded(library, statsAggregatorStageConf, ownIssues);
    }

    // upgrade error stage if present and if necessary
    StageConfiguration errorStageConf = pipelineConf.getErrorStage();
    if (errorStageConf != null) {
      errorStageConf = upgradeIfNeeded(library, errorStageConf, ownIssues);
    }

    // Pipeline start and stop events
    List<StageConfiguration> startEventStages = new ArrayList<>();
    if(pipelineConf.getStartEventStages() != null) {
      for(StageConfiguration conf : pipelineConf.getStartEventStages()) {
        StageConfiguration upgradedConf = upgradeIfNeeded(library, conf, ownIssues);
        startEventStages.add(upgradedConf);
      }
    }
    List<StageConfiguration> stopEventStages = new ArrayList<>();
    if(pipelineConf.getStopEventStages() != null) {
      for(StageConfiguration conf : pipelineConf.getStopEventStages()) {
        StageConfiguration upgradedConf = upgradeIfNeeded(library, conf, ownIssues);
        stopEventStages.add(upgradedConf);
      }
    }

    // Test origin
    StageConfiguration testOrigin = pipelineConf.getTestOriginStage();
    if(testOrigin != null) {
      testOrigin = upgradeIfNeeded(library, testOrigin, ownIssues);
    }
    // upgrade stages;
    for (StageConfiguration stageConf : pipelineConf.getStages()) {
      stageConf = upgradeIfNeeded(library, stageConf, issues);
      if (stageConf != null) {
        stageConfs.add(stageConf);
      }
    }

    // if ownIssues > 0 we had an issue upgrading, we wont touch the pipelineConf and return null
    if (ownIssues.isEmpty()) {
      pipelineConf.setErrorStage(errorStageConf);
      pipelineConf.setStatsAggregatorStage(statsAggregatorStageConf);
      pipelineConf.setStartEventStages(startEventStages);
      pipelineConf.setStopEventStages(stopEventStages);
      pipelineConf.setTestOriginStage(testOrigin);
      pipelineConf.setStages(stageConfs);

      if(errorStageConf != null) {
        pipelineConfs.addConfig(new Config("badRecordsHandling", stageToUISelect(errorStageConf)));
      }
      if(statsAggregatorStageConf != null) {
        pipelineConfs.addConfig(new Config("statsAggregatorStage", stageToUISelect(statsAggregatorStageConf)));
      }
      if(startEventStages.size() == 1) {
        pipelineConfs.addConfig(new Config("startEventStage", stageToUISelect(startEventStages.get(0))));
      }
      if(stopEventStages.size() == 1) {
        pipelineConfs.addConfig(new Config("stopEventStage", stageToUISelect(stopEventStages.get(0))));
      }
      if(testOrigin != null) {
        pipelineConfs.addConfig(new Config("testOriginStage", stageToUISelect(testOrigin)));
      }

      pipelineConf.setConfiguration(pipelineConfs.getConfiguration());
      pipelineConf.setVersion(pipelineConfs.getStageVersion());
    } else {
      issues.addAll(ownIssues);
      pipelineConf = null;
    }

    return pipelineConf;
  }

  static String stageToUISelect(StageConfiguration stageConf) {
    return stageConf.getLibrary() + "::" + stageConf.getStageName() + "::" + stageConf.getStageVersion();
  }

  /**
   * Upgrade whole Stage configuration, including all services if needed. Convenience method that will lookup stage
   * definition from the library.
   *
   * This method is idempotent.
   */
  static StageConfiguration upgradeIfNeeded(StageLibraryTask library, StageConfiguration conf, List<Issue> issues) {
    return upgradeIfNeeded(
        library,
        library.getStage(conf.getLibrary(), conf.getStageName(), false),
        conf,
        issues
    );
  }

  /**
   * Upgrade whole Stage configuration, including all services if needed.
   *
   * This method is idempotent.
   */
  static StageConfiguration upgradeIfNeeded(StageLibraryTask library, StageDefinition def, StageConfiguration conf, List<Issue> issues) {
    IssueCreator issueCreator = IssueCreator.getStage(conf.getInstanceName());
    int fromVersion = conf.getStageVersion();
    int toVersion = def.getVersion();
    try {

      // Firstly upgrade stage itself (register any new services)
      upgradeStageIfNeeded(def, conf, issueCreator, issues);

      // And then upgrade all it's services
      conf.getServices().forEach(serviceConf -> upgradeServicesIfNeeded(
          library,
          conf,
          serviceConf,
          issueCreator.forService(serviceConf.getService().getName()),
          issues
      ));
    } catch (Exception ex) {
      LOG.error("Unknown exception during upgrade: " + ex, ex);
      issues.add(issueCreator.create(
          ContainerError.CONTAINER_0900,
          fromVersion,
          toVersion,
          ex.toString()
      ));
    }

    return conf;
  }

  /**
   * Internal method that will upgrade service configuration if needed.
   *
   * This method is idempotent.
   */
  private static ServiceConfiguration upgradeServicesIfNeeded(
      StageLibraryTask library,
      StageConfiguration stageConf,
      ServiceConfiguration conf,
      IssueCreator issueCreator,
      List<Issue> issues
  ) {
    ServiceDefinition def = library.getServiceDefinition(conf.getService(), false);
    if (def == null) {
      issues.add(issueCreator.create(ContainerError.CONTAINER_0903, conf.getService().getName()));
    }

    int fromVersion = conf.getServiceVersion();
    int toVersion = def.getVersion();

    // In case we don't need an upgrade
    if(!needsUpgrade(toVersion, fromVersion, issueCreator, issues)) {
      return conf;
    }

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      LOG.warn("Upgrading service instance from version '{}' to version '{}'", conf.getServiceVersion(), def.getVersion());

      UpgradeContext upgradeContext = new UpgradeContext(
          "",
          def.getName(),
          stageConf.getInstanceName(),
          fromVersion,
          toVersion
      );

      List<Config> configs = def.getUpgrader().upgrade(conf.getConfiguration(), upgradeContext);

      if(!upgradeContext.registeredServices.isEmpty()) {
        throw new StageException(ContainerError.CONTAINER_0904);
      }

      conf.setServiceVersion(toVersion);
      conf.setConfig(configs);
    } catch (StageException ex) {
      issues.add(issueCreator.create(ex.getErrorCode(), ex.getParams()));
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }

    return conf;
  }

  /**
   * Internal method that will upgrade only Stage configuration - not the associated services - and only if needed.
   *
   * This method is idempotent.
   */
  static private void upgradeStageIfNeeded(
      StageDefinition def,
      StageConfiguration conf,
      IssueCreator issueCreator,
      List<Issue> issues
  ) {
    int fromVersion = conf.getStageVersion();
    int toVersion = def.getVersion();

    // In case we don't need an upgrade
    if(!needsUpgrade(toVersion, fromVersion, IssueCreator.getStage(conf.getInstanceName()), issues)) {
      return;
    }

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(def.getStageClassLoader());
      LOG.info("Upgrading stage '{}' instance '{}' from version '{}' to version '{}'", conf.getStageName(), conf.getInstanceName(), fromVersion, toVersion);

      UpgradeContext upgradeContext = new UpgradeContext(
          def.getLibrary(),
          def.getName(),
          conf.getInstanceName(),
          fromVersion,
          toVersion
      );
      List<Config> configs = def.getUpgrader().upgrade(conf.getConfiguration(), upgradeContext);

      conf.setStageVersion(def.getVersion());
      conf.setConfig(configs);

      // Propagate newly registered services to the StageConfiguration
      if(!upgradeContext.registeredServices.isEmpty()) {
        List<ServiceConfiguration> services = new ArrayList<>();
        services.addAll(conf.getServices());

        // Version -1 is special to note that this version has been created by stage and not by the service itself
        upgradeContext.registeredServices
            .forEach((s, c) -> services.add(new ServiceConfiguration(s, -1, c)));

        conf.setServices(services);

      }
    } catch (StageException ex) {
      issues.add(issueCreator.create(ex.getErrorCode(), ex.getParams()));
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  private static class UpgradeContext implements StageUpgrader.Context {

    private final String library;
    private final String stageName;
    private final String stageInstanceName;
    private final int fromVersion;
    private final int toVersion;
    private final Map<Class, List<Config>> registeredServices;

    UpgradeContext(
        String library,
        String stageName,
        String stageInstanceName,
        int fromVersion,
        int toVersion
    ) {
      this.library = library;
      this.stageName = stageName;
      this.stageInstanceName = stageInstanceName;
      this.fromVersion = fromVersion;
      this.toVersion = toVersion;
      this.registeredServices = new HashMap<>();
    }

    @Override
    public String getLibrary() {
      return library;
    }

    @Override
    public String getStageName() {
      return stageName;
    }

    @Override
    public String getStageInstance() {
      return stageInstanceName;
    }

    @Override
    public int getFromVersion() {
      return fromVersion;
    }

    @Override
    public int getToVersion() {
      return toVersion;
    }

    @Override
    public void registerService(Class service, List<Config> configs) {
      registeredServices.put(service, configs);
    }
  }

}
