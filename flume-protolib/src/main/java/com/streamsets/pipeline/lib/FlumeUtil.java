/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.flume.FlumeErrors;

import java.util.List;
import java.util.Map;

public class FlumeUtil {

  public static boolean validateHostConfig(List<Stage.ConfigIssue> issues, Map<String, String> flumeHostsConfig,
                                                    String confiGroupName, String configName, Stage.Context context) {
    boolean valid = true;
    if(flumeHostsConfig == null || flumeHostsConfig.isEmpty()) {
      issues.add(context.createConfigIssue(confiGroupName, configName,
        FlumeErrors.FLUME_101, configName));
      return false;
    }

    for(Map.Entry<String, String> e : flumeHostsConfig.entrySet()) {
      String hostAlias = e.getKey();
      if(hostAlias == null || hostAlias.isEmpty()) {
        issues.add(context.createConfigIssue(confiGroupName, configName,
          FlumeErrors.FLUME_102, configName));
      }
      String address = e.getValue();
      String[] hostAndPort = address.split(":");
      if(hostAndPort.length != 2) {
        issues.add(context.createConfigIssue(confiGroupName, configName, FlumeErrors.FLUME_103, address));
      } else {
        try {
          Integer.parseInt(hostAndPort[1]);
        } catch (NumberFormatException ex) {
          issues.add(context.createConfigIssue(confiGroupName, configName, FlumeErrors.FLUME_103, address));
          valid = false;
        }
      }
    }
    return valid;
  }
}
