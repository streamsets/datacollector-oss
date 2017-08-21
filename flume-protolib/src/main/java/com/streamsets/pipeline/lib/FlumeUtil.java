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
package com.streamsets.pipeline.lib;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.flume.FlumeErrors;

import java.util.List;
import java.util.Map;

public class FlumeUtil {

  private FlumeUtil() {}

  public static boolean validateHostConfig(List<Stage.ConfigIssue> issues, Map<String, String> flumeHostsConfig,
                                                    String configGroupName, String configName, Stage.Context context) {
    boolean valid = true;
    if(flumeHostsConfig == null || flumeHostsConfig.isEmpty()) {
      issues.add(context.createConfigIssue(configGroupName, configName,
        FlumeErrors.FLUME_101, configName));
      return false;
    }

    for(Map.Entry<String, String> e : flumeHostsConfig.entrySet()) {
      String hostAlias = e.getKey();
      if(hostAlias == null || hostAlias.isEmpty()) {
        issues.add(context.createConfigIssue(configGroupName, configName,
          FlumeErrors.FLUME_102, configName));
        valid = false;
      }
      String address = e.getValue();
      String[] hostAndPort = address.split(":");
      if(hostAndPort.length != 2) {
        issues.add(context.createConfigIssue(configGroupName, configName, FlumeErrors.FLUME_103, address));
        valid = false;
      } else {
        try {
          Integer.parseInt(hostAndPort[1]);
        } catch (NumberFormatException ex) {
          issues.add(context.createConfigIssue(configGroupName, configName, FlumeErrors.FLUME_103, address));
          valid = false;
        }
      }
    }
    return valid;
  }
}
