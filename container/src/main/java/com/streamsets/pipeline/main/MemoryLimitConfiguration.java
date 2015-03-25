/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.main;

import com.streamsets.pipeline.api.impl.Utils;

import java.util.HashMap;
import java.util.Map;

public class MemoryLimitConfiguration {
  private static final String DEFAULT = "default";
  private long defaultValue = 1024 * 1024 * 10; // 10MiB
  private final Map<String, Long> memoryLimitConfigs;

  public MemoryLimitConfiguration(Map<String, String> memoryLimitConfigs) {
    this.memoryLimitConfigs = new HashMap<>();
    for (Map.Entry<String, String> entry : memoryLimitConfigs.entrySet()) {
      try {
        if (DEFAULT.equalsIgnoreCase(entry.getKey())) {
          this.defaultValue = Utils.humanReadableToBytes(entry.getValue());
        } else {
          this.memoryLimitConfigs.put(entry.getKey(), Utils.humanReadableToBytes(entry.getValue()));
        }
      } catch (NumberFormatException e) {
        String msg = "Invalid configuration at key " + RuntimeModule.STAGE_MEMORY_LIMIT_PREFIX + entry.getKey()
          + ": " + e;
        throw new IllegalArgumentException(msg, e);
      }
    }
  }

  public long getMemoryLimit(String stageName, String stageVersion) {
    String key = stageName + "." + stageVersion;
    if (memoryLimitConfigs.containsKey(key)) {
      return memoryLimitConfigs.get(key);
    } else if (memoryLimitConfigs.containsKey(stageName)) {
      return memoryLimitConfigs.get(stageName);
    }
    return defaultValue;
  }

  public static MemoryLimitConfiguration empty() {
    return new MemoryLimitConfiguration(new HashMap<String, String>());
  }
}
