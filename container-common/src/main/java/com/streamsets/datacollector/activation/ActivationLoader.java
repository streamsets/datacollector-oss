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
package com.streamsets.datacollector.activation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;

/**
 * Loads the Activation implementation if any is present, by default there is none.
 * <p/>
 * If there is no Activation implementation present it returns a NOP one.
 */
public class ActivationLoader {
  private final static Logger LOG = LoggerFactory.getLogger(ActivationLoader.class);
  private final RuntimeInfo runtimeInfo;

  public ActivationLoader(RuntimeInfo runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
  }

  public Activation getActivation() {
    final Activation activation;
    ServiceLoader<Activation> serviceLoader = ServiceLoader.load(Activation.class);
    List<Activation> list = ImmutableList.copyOf(serviceLoader.iterator());
    if (runtimeInfo.isDPMEnabled()) {
      activation = new NopActivation();
      LOG.debug("Control Hub is enabled, using {}", activation.getClass().getName());
    } else if (list.isEmpty()) {
      activation = new NopActivation();
      LOG.debug("No activation service available, using {}", activation.getClass().getName());
    } else if (list.size() == 1) {
      activation = list.get(0);
      LOG.debug("Found activation service {}", activation.getClass().getName());
    } else {
      List<String> names = Lists.transform(list, element -> element.getClass().getName());
      throw new RuntimeException(Utils.format(
          "There cannot be more than one Activation service, found '{}': {}",
          list.size(),
          names
      ));
    }
    LOG.debug("Initializing");
    activation.init(runtimeInfo);
    LOG.debug("Initialized");
    if (activation.isEnabled()) {
      Activation.Info info = activation.getInfo();
      if (!info.isValid()) {
        LOG.info("Activation enabled, activation is not valid");
      } else if (info.getExpiration() == -1) {
        LOG.info("Activation enabled, activation is valid and it does not expire");
      } else {
        long daysToExpire = TimeUnit.MILLISECONDS.toDays(info.getExpiration() - System.currentTimeMillis() );
        if (daysToExpire < 0) {
          LOG.info("Bypass activation because SDC contains only basic stage libraries.");
        } else {
          LOG.info("Activation enabled, activation is valid and it expires in '{}' days", daysToExpire);
        }
      }
    } else {
      LOG.debug("Activation disabled");
    }
    return activation;
  }


}
