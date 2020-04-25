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
package com.streamsets.datacollector.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class SysInfo {
  private static final Logger LOG = LoggerFactory.getLogger(SysInfo.class);

  // sanity check for any info passed via env that goes directly into telemetry
  private static final String VALID_ENV_VAR_VALUE_PATTERN = "[a-zA-Z_\\-]{3,255}";
  /** Blacklist of json keys, values are redacted */
  private static final Set<String> KEYS_TO_REDACT = ImmutableSet.of(
      "customdata",
      "tags",
      "tagslist");
  /** Heuristic pattern applied to keys to determine whether to redact the values */
  private static final String KEY_PATTERN_TO_REDACT = ".*(key|secret|password|credential).*";

  public static final String UNKNOWN = "UNKNOWN";
  public static final String REDACTED_TEXT = "**REDACTED**";
  public static final String ENV_CLD_PROVIDER = "CLD_PROVIDER";
  public static final String ENV_SDC_DISTRIBUTION_CHANNEL = "SDC_DISTRIBUTION_CHANNEL";

  private static final long SCRIPT_CUTOFF_PERIOD = TimeUnit.SECONDS.toMillis(60);

  @VisibleForTesting
  static final String CLOUD_METADATA_SCRIPT_ID = "cloudMetadata";
  @VisibleForTesting
  static final String SYS_V_CORES_SCRIPT_ID = "sysVCores";
  @VisibleForTesting
  static final String SYS_MEM_SCRIPT_ID = "sysMem";
  @VisibleForTesting
  static final String CLOUD_PROVIDER_SCRIPT_ID = "cloudProvider";

  // we use futures since it can take a while to determine what these are, and by the time we need them they should
  // hopefully be known

  // This is a String rather than an enum so that we don't have to update SDC to add a new provider to telemetry
  private final Future<String> cloudProvider;
  private final Future<Map<String, Object>> cloudMetadata;
  private final Future<Long> sysVCores;
  private final Future<Long> sysMem;
  private final String libExecDir;

  public SysInfo(RuntimeInfo runtimeInfo) {
    String envCldProvider = System.getenv(ENV_CLD_PROVIDER);
    libExecDir = runtimeInfo.getLibexecDir();
    if (envCldProvider != null) {
      cloudProvider = hardcodedFuture(envCldProvider);
    } else {
      cloudProvider = createScriptFuture(CLOUD_PROVIDER_SCRIPT_ID,
          new File(libExecDir, "_sys_info_util").getPath() + " get_cloud_provider",
          new StdOutConverter<String>() {
            @Override
            public String processStdOut(String stdout) {
              if (stdout.matches(VALID_ENV_VAR_VALUE_PATTERN)) {
                return stdout;
              } else {
                return UNKNOWN;
              }
            }
          });
    }
    cloudMetadata = createScriptFuture(CLOUD_METADATA_SCRIPT_ID,
        new File(libExecDir, "_sys_info_util").getPath() + " get_cloud_metadata",
        new SysInfo.StdOutConverter<Map<String, Object>>() {
          @Override
          public Map<String, Object> processStdOut(String stdout) {
            return parseAndRedactMetadata(stdout.trim());
          }
        });
    sysVCores = createScriptFuture(
        SYS_V_CORES_SCRIPT_ID,
        "sysctl -n hw.ncpu || lscpu -p 2>/dev/null | egrep -v '^#' | wc -l",
        new StdOutConverter<Long>() {
          @Override
          public Long processStdOut(String stdout) {
            return Long.parseLong(stdout);
          }
        });
    sysMem = createScriptFuture(
        SYS_MEM_SCRIPT_ID, "sysctl -n hw.memsize || awk '/^MemTotal:/{print $2}' /proc/meminfo",
        new StdOutConverter<Long>() {
          @Override
          public Long processStdOut(String stdout) {
            return Long.parseLong(stdout);
          }
        });
  }

  @VisibleForTesting
  static Map<String, Object> parseAndRedactMetadata(String metadata) {
    if (Strings.isNullOrEmpty(metadata)) {
      return null; // json serialization doesn't like empty maps
    }
    try {
      Map<String, Object> ret = ObjectMapperFactory.get().readValue(metadata, new TypeReference<Map<String, Object>>(){});
      return redactMap(ret);
    } catch (IOException e) {
      LOG.error("Unable to parse metadata", e);
      return ImmutableMap.of();
    }
  }

  private static Map<String, Object> redactMap(Map<String, Object> map) {
    for (String key : ImmutableSet.copyOf(map.keySet())) {
      String lowerKey = key.toLowerCase(Locale.US);
      if (KEYS_TO_REDACT.contains(lowerKey) || lowerKey.matches(KEY_PATTERN_TO_REDACT)) {
        map.put(key, REDACTED_TEXT);
      } else {
        map.put(key, redactValue(map.get(key)));
      }
    }
    return map;
  }

  private static List<Object> redactList(List<Object> list) {
    return list.stream().map(SysInfo::redactValue).collect(Collectors.toList());
  }

  private static Object redactValue(Object value) {
    if (value instanceof Map) {
      return redactMap((Map<String, Object>) value);
    } else if (value instanceof List) {
      return redactList((List<Object>) value);
    }
    return value;
  }

  public String getCloudProvider() {
    return getOrNull(cloudProvider);
  }

  public Map<String, Object> getCloudMetadata() {
    return getOrNull(cloudMetadata);
  }

  public String getDistributionChannel() {
    String ret = System.getenv(ENV_SDC_DISTRIBUTION_CHANNEL);
    return ret != null ? ret : UNKNOWN;
  }

  public String getJavaVersion() {
    return System.getProperty("java.version");
  }

  /**
   * @return whether JVM is configured to use container memory limit for heap. If not, then it will report physical host
   * instead of container limits. Only available in JDK 9 and JDK 8u131+
   */
  public boolean isUseCGroupMemorySet() {
    // technically this could be configured some other way, but we'll always use the command line flag
    return ManagementFactory.getRuntimeMXBean().getInputArguments().contains("-XX:+UnlockExperimentalVMOptions");
  }

  /**
   * @return number of cores available to the JVM
   */
  public long getJvmCores() {
    return Runtime.getRuntime().availableProcessors();
  }

  public long getJvmMaxHeap() {
    return Runtime.getRuntime().maxMemory();
  }

  public long getJvmPeakMetaspaceUsed() {
    return getMetaspaceMxBean()
        .map(MemoryPoolMXBean::getPeakUsage)
        .map(MemoryUsage::getUsed)
        .orElse(-1L);
  }

  public Long getSysVCores() {
    return getOrNull(sysVCores);
  }

  public Long getSysMem() {
    return getOrNull(sysMem);
  }

  private static Optional<MemoryPoolMXBean> getMetaspaceMxBean() {
    for (MemoryPoolMXBean bean : ManagementFactory.getMemoryPoolMXBeans()) {
      if ("Metaspace".equals(bean.getName())) {
        return Optional.of(bean);
      }
    }
    return Optional.empty(); // can't really happen but we'll be really safe
  }

  private <T> T getOrNull(Future<T> f) {
    try {
      // generally ok if we return null initially and a later update gets the real data, but keep a short timeout so
      // bootup isn't overly impacted
      return f.get(1, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.warn("Exception getting future", e);
      return null;
    }
  }

  private <T> Future<T> createScriptFuture(String name, String script, StdOutConverter<T> converter) {
    return createScriptFuture(name, script, null, converter);
  }

  @VisibleForTesting
  protected <T> Future<T> createScriptFuture(
      String name,
      String script,
      Map<String, String> additionalEnv,
      StdOutConverter<T> converter) {
    try {
      return new ScriptProcessFuture<T>(name, script, additionalEnv, converter);
    } catch (IOException e) {
      LOG.warn("Unable to exec determineCloudProvider", e);
      return hardcodedFuture(null);
    }
  }

  @VisibleForTesting
  protected interface StdOutConverter<T> {
    /**
     * Transform the process stdout into the desired return type
     * @param stdout
     * @return
     */
    T processStdOut(String stdout);
  }

  @VisibleForTesting
  class ScriptProcessFuture<T> implements Future<T> {
    private final Process p;
    private final String name;
    private final long cutoffTime;
    private final StdOutConverter<T> converter;
    private boolean cancelled = false;
    private boolean isResultCached = false;
    private T cachedResult = null;

    ScriptProcessFuture(String name, String script, Map<String, String> additionalEnv, StdOutConverter<T> converter) throws IOException {
      String[] args;
      if (!script.startsWith(libExecDir)) {
        args = new String[]{"/bin/bash", "-c", script};
      } else {
        // this is a named script file, don't need -c (that will result in permission denied)
        List<String> argsList = new ArrayList<>();
        argsList.add("/bin/bash");
        // split program and args
        argsList.addAll(Arrays.asList(script.split(" ")));
        args = argsList.toArray(new String[argsList.size()]);
      }
      ProcessBuilder pb = new ProcessBuilder(args);
      if (additionalEnv != null) {
        pb.environment().putAll(additionalEnv);
      }
      this.p = pb.start();
      this.name = name;
      this.converter = converter;
      this.cutoffTime = System.currentTimeMillis() + SCRIPT_CUTOFF_PERIOD;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      if (getP().isAlive()) {
        getP().destroyForcibly();
        cancelled = true;
        return true;
      }
      return false;
    }

    @VisibleForTesting
    Process getP() {
      return p;
    }

    @Override
    public boolean isCancelled() {
      return cancelled;
    }

    @Override
    public boolean isDone() {
      return !getP().isAlive();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      return internalGet(Math.max(0, getCutoffTime() - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
    }

    @Override
    public T get(long timeout, @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return internalGet(timeout, unit);
    }

    private T internalGet(long timeout, TimeUnit unit) throws InterruptedException {
      if (isResultCached) {
        return cachedResult;
      }
      try {
        synchronized(getP()) {
          if (!getP().waitFor(timeout, unit)) {
            LOG.error("Process {} did not finish before timeout, using null", name);
            finalizeNullIfRunningTooLong();
            return null;
          };
          if (getP().exitValue() != 0) {
            LOG.warn("Subprocess {} exited with code {} and stderr: {}",
                name,
                getP().exitValue(),
                IOUtils.toString(getP().getErrorStream()).trim());
          }
          cachedResult = converter.processStdOut(getProcessStdOut(getP()));
          isResultCached = true;
        }
        return cachedResult;
      } catch (IOException e) {
        LOG.warn("Unable to get stdout for process " + name, e);
        finalizeNullIfRunningTooLong();
        return null;
      }
    }

    private void finalizeNullIfRunningTooLong() {
      if (System.currentTimeMillis() > getCutoffTime()) {
        // if this has been running for a long time, consider the null result final
        try {
          cancel(true);
        } catch (Exception e) {
          LOG.error("Could not cancel process {}", name);
        }
        isResultCached = true;
        cachedResult = null;
      }
    }

    @VisibleForTesting
    long getCutoffTime() {
      return cutoffTime;
    }
  }

  @VisibleForTesting
  protected  <T> Future<T> hardcodedFuture(T value) {
    return new Future<T>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return false;
      }

      @Override
      public T get() throws InterruptedException, ExecutionException {
        return value;
      }

      @Override
      public T get(long timeout, @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return value;
      }
    };
  }

  /**
   * Gets a process' stdout, and trims it, then returns it. Does not wait for the process to finish.
   * @param p
   * @return
   * @throws IOException
   */
  private static String getProcessStdOut(Process p) throws IOException {
    return IOUtils.toString(p.getInputStream()).trim();
  }

  @Override
  public String toString() {
    return "SysInfo: " + toMap().toString();
  }

  public Map<String, Object> toMap() {
    try {
      ObjectMapper objectMapper = ObjectMapperFactory.get();
      String json = objectMapper.writeValueAsString(this);
      return objectMapper.readValue(json, new TypeReference<Map<String, Object>>(){});
    } catch (IOException e) {
      LOG.error("Unable to serialize SysInfo", e);
      return null;
    }
  }
}
