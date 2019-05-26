/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.antennadoctor;

import com.streamsets.datacollector.antennadoctor.bean.AntennaDoctorRuleBean;
import com.streamsets.datacollector.antennadoctor.engine.AntennaDoctorEngine;
import com.streamsets.datacollector.antennadoctor.engine.context.AntennaDoctorContext;
import com.streamsets.datacollector.antennadoctor.engine.context.AntennaDoctorStageContext;
import com.streamsets.datacollector.antennadoctor.storage.AntennaDoctorStorage;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.AntennaDoctorMessage;
import com.streamsets.pipeline.api.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class AntennaDoctor extends AbstractTask implements AntennaDoctorTask, AntennaDoctorStorage.NewRulesDelegate {
  private static final Logger LOG = LoggerFactory.getLogger(AntennaDoctor.class);

  /**
   * Static instance so that it's easy to retrieve the Doctor across the code base.
   */
  private static AntennaDoctor INSTANCE = null;
  public static AntennaDoctor getInstance() {
    return INSTANCE;
  }

  /**
   * Name of the product in which Antenna Doctor is loaded.
   */
  private final String productName;

  /**
   * Main storage that is initialized during init phase.
   */
  private AntennaDoctorStorage storage;

  /**
   * Main engine used to classify issues.
   *
   * Replaced on each rule reload.
   */
  private AntennaDoctorEngine engine;

  /**
   * Context that is relevant for the engine.
   */
  private final AntennaDoctorContext context;

  public AntennaDoctor(
      String productName,
      RuntimeInfo runtimeInfo,
      BuildInfo buildInfo,
      Configuration configuration,
      StageLibraryTask stageLibraryTask
  ) {
    super("Antenna Doctor for " + productName);
    this.productName = productName;
    this.context = new AntennaDoctorContext(
      runtimeInfo,
      buildInfo,
      configuration,
      stageLibraryTask
    );
    INSTANCE = this;
  }

  @Override
  protected void initTask() {
    LOG.info("Initializing Antenna Doctor");
    super.initTask();

    if(!context.getConfiguration().get(AntennaDoctorConstants.CONF_ENABLE, AntennaDoctorConstants.DEFAULT_ENABLE)) {
      LOG.info("Antenna Doctor is disabled");
      return;
    }

    this.storage = new AntennaDoctorStorage(
      productName,
      context.getBuildInfo(),
      context.getConfiguration(),
      context.getRuntimeInfo().getDataDir(),
      this
    );
    this.storage.init();
  }

  @Override
  protected void stopTask() {
    LOG.info("Stopping Antenna Doctor");
    INSTANCE = null;
    if(storage != null) {
      storage.stop();
      storage = null;
    }
    super.stopTask();
  }

  @Override
  public void loadNewRules(List<AntennaDoctorRuleBean> rules) {
    this.engine = new AntennaDoctorEngine(context, rules);
  }

  @Override
  public AntennaDoctorContext getContext() {
    return context;
  }

  @Override
  public List<AntennaDoctorMessage> onStage(AntennaDoctorStageContext context, Exception exception) {
    AntennaDoctorEngine engine = this.engine;
    if(engine != null) {
      return engine.onStage(context, exception);
    }

    return Collections.emptyList();
  }

  @Override
  public List<AntennaDoctorMessage> onStage(AntennaDoctorStageContext context, ErrorCode errorCode, Object... args) {
    AntennaDoctorEngine engine = this.engine;
    if(engine != null) {
      return engine.onStage(context, errorCode, args);
    }

    return Collections.emptyList();
  }

  @Override
  public List<AntennaDoctorMessage> onStage(AntennaDoctorStageContext context, String errorMessage) {
    AntennaDoctorEngine engine = this.engine;
    if(engine != null) {
      return engine.onStage(context, errorMessage);
    }

    return Collections.emptyList();
  }

  @Override
  public List<AntennaDoctorMessage> onRest(ErrorCode errorCode, Object... args) {
    AntennaDoctorEngine engine = this.engine;
    if(engine != null) {
      return engine.onRest(getContext(), errorCode, args);
    }

    return Collections.emptyList();
  }

  @Override
  public List<AntennaDoctorMessage> onRest(Exception exception) {
    AntennaDoctorEngine engine = this.engine;
    if(engine != null) {
      return engine.onRest(getContext(), exception);
    }

    return Collections.emptyList();
  }

  @Override
  public List<AntennaDoctorMessage> onValidation(AntennaDoctorStageContext context, String groupName, String configName, ErrorCode errorCode, Object... args) {
    AntennaDoctorEngine engine = this.engine;
    if(engine != null) {
      return engine.onValidation(context, groupName, configName, errorCode, args);
    }

    return Collections.emptyList();
  }
}
