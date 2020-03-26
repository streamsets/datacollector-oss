/*
 * Copyright 2020 StreamSets Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.pipeline.SDCClassLoader;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This activation is valid if SDC contains only whitelisted stage libraries;
 * otherwise, it delegates to the given activation. Following stage libraries
 * are whitelisted which are in the core package.
 * <ul>
 *   <li>streamsets-datacollector-basic-lib</li>
 *   <li>streamsets-datacollector-dataformats-lib</li>
 *   <li>streamsets-datacollector-dev-lib</li>
 *   <li>streamsets-datacollector-stats-lib</li>
 *   <li>streamsets-datacollector-windows-lib</li>
 * </ul>
 */
public class WhitelistActivation implements Activation {

  private static final Set<String> WHITELIST = ImmutableSet.of(
      "streamsets-datacollector-basic-lib",
      "streamsets-datacollector-dataformats-lib",
      "streamsets-datacollector-dev-lib",
      "streamsets-datacollector-stats-lib",
      "streamsets-datacollector-windows-lib"
  );
  private final Activation activation;
  private Info whitelistedInfo;

  public WhitelistActivation(Activation activation) {
    this.activation = activation;
  }

  @Override
  public void init(RuntimeInfo runtimeInfo) {
    activation.init(runtimeInfo);
    Info info = activation.getInfo();
    boolean coreOnly = !runtimeInfo.getStageLibraryClassLoaders().stream()
        .filter(cl -> cl instanceof SDCClassLoader)
        .map(cl -> ((SDCClassLoader) cl).getName())
        .filter(name -> !WHITELIST.contains(name))
        .findAny().isPresent();
    this.whitelistedInfo = new BypassActivationInfo(info, coreOnly);
  }

  @Override
  public boolean isEnabled() {
    return activation.isEnabled();
  }

  @Override
  public Info getInfo() {
    return whitelistedInfo;
  }

  @Override
  public void setActivationKey(String activationKey) {
    activation.setActivationKey(activationKey);
  }

  /**
   * @return the underlying activation
   */
  @VisibleForTesting
  <T extends Activation> T getActivation() {
    return (T) activation;
  }

  /**
   * The Activation.Info which is always valid if bypass is true.
   */
  private static class BypassActivationInfo implements Info {

    private final Info info;
    private final boolean bypass;

    public BypassActivationInfo(Info info, boolean bypass) {
      this.info = info;
      this.bypass = bypass;
    }

    @Override
    public String getType() {
      return info.getType();
    }

    @Override
    public boolean isValid() {
      return bypass || info.isValid();
    }

    @Override
    public long getFirstUse() {
      return info.getFirstUse();
    }

    @Override
    public long getExpiration() {
      return info.getExpiration();
    }

    @Override
    public String getSdcId() {
      return info.getSdcId();
    }

    @Override
    public String getUserInfo() {
      return info.getUserInfo();
    }

    @Override
    public List<String> getValidSdcIds() {
      return info.getValidSdcIds();
    }

    @Override
    public Map<String, Object> getAdditionalInfo() {
      return info.getAdditionalInfo();
    }
  }
}
