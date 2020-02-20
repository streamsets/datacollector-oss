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

package com.streamsets.service.sshtunnel;

import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseService;
import com.streamsets.pipeline.api.service.sshtunnel.SshTunnelService;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DummySshService extends BaseService implements SshTunnelService {

  public DummySshService() {
    // Empty constructor to hide the inherited one
  }

  @Override
  public List<ConfigIssue> init() {
    return Collections.emptyList(); // Return empty list since we don't need to init anything
  }

  @Override
  public boolean isEnabled() {
    return true; // Always return true to avoid non
  }

  @Override
  public Map<HostPort, HostPort> start(List<HostPort> targetHostsPorts) {
    Map<HostPort, HostPort> hostPortMapping = new HashMap<>();
    for (HostPort entry : targetHostsPorts){
      hostPortMapping.put(entry, entry);
    }
    return hostPortMapping;
  }

  @Override
  public void healthCheck() throws StageException {
    // Everything is OK
  }

  @Override
  public void stop() {
    // Do nothing
  }

  @Override
  public void destroy() {
    // Do nothing
  }
}
