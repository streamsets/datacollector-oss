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
import com.streamsets.datacollector.antennadoctor.storage.AntennaDoctorStorage;
import com.streamsets.datacollector.task.AbstractTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AntennaDoctor extends AbstractTask implements AntennaDoctorTask, AntennaDoctorStorage.NewRulesDelegate {
  private static final Logger LOG = LoggerFactory.getLogger(AntennaDoctor.class);

  private AntennaDoctorStorage storage;

  public AntennaDoctor() {
    super("Antenna Doctor");
  }

  @Override
  protected void initTask() {
    LOG.info("Initializing Antenna Doctor");
    super.initTask();

    this.storage = new AntennaDoctorStorage(this);
    this.storage.init();
  }

  @Override
  protected void stopTask() {
    LOG.info("Stopping Antenna Doctor");
    super.stopTask();
  }

  @Override
  public void loadNewRules(List<AntennaDoctorRuleBean> rules) {
    LOG.info("Loaded {} rules", rules.size());
  }
}
