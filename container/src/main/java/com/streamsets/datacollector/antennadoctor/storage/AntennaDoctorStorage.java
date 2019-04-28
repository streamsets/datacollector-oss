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
package com.streamsets.datacollector.antennadoctor.storage;

import com.google.common.io.Resources;
import com.streamsets.datacollector.antennadoctor.bean.AntennaDoctorRuleBean;
import com.streamsets.datacollector.antennadoctor.bean.AntennaDoctorStorageBean;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.task.AbstractTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Storage module for Antenna Doctor.
 *
 * The storage currently only loads static storage from build-in jar file. However in the future we will add support
 * for loading even remote tools.
 */
public class AntennaDoctorStorage extends AbstractTask {
  private static final Logger LOG = LoggerFactory.getLogger(AntennaDoctorStorage.class);

  /**
   * Delegate when a new rules are available.
   *
   * This method is always called with all rules together (no incremental rule application here).
   */
  public interface NewRulesDelegate {
    void loadNewRules(List<AntennaDoctorRuleBean> rules);
  }

  private final NewRulesDelegate delegate;

  public AntennaDoctorStorage(NewRulesDelegate delegate) {
    super("Antenna Doctor Storage");
    this.delegate = delegate;
  }

  @Override
  protected void initTask() {
    try {
      AntennaDoctorStorageBean storageBean = ObjectMapperFactory.get().readValue(Resources.getResource(AntennaDoctorStorage.class, "antenna-doctor-rules.json"), AntennaDoctorStorageBean.class);
      delegate.loadNewRules(storageBean.getRules());
    } catch (Throwable e) {
      LOG.error("Can't load default rule list, Antenna Doctor will be disabled", e);
    }
  }
}
