/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.main;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.streamsets.pipeline.api.impl.Utils;

import com.streamsets.pipeline.util.AuthzRole;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.io.File;

public class RuntimeInfo {
  /**
   * Note this differs from the Pipeline level enum named ExecutionMode
   */
  public enum ExecutionMode { CLUSTER, STANDALONE, SLAVE }

  public static final String SPLITTER = "|";
  public static final String CONFIG_DIR = ".conf.dir";
  public static final String DATA_DIR = ".data.dir";
  public static final String LOG_DIR = ".log.dir";
  public static final String STATIC_WEB_DIR = ".static-web.dir";
  public static final String TRANSIENT_ENVIRONMENT = "sdc.transient-env";
  public static final String UNDEF = "UNDEF";
  public static final String CALLBACK_URL = "/rest/v1/cluster/callback";
  private final static String USER_ROLE = "user";

  public static final String LOG4J_CONFIGURATION_URL_ATTR = "log4j.configuration.url";
  private static final String LOG4J_PROPERTIES = "-log4j.properties";

  private final MetricRegistry metrics;
  private final List<? extends ClassLoader> stageLibraryClassLoaders;
  private String id;
  private String httpUrl;
  private final Map<String, Object> attributes;
  private Runnable shutdownRunnable;
  private Map<String, String> authenticationTokens;
  private final String propertyPrefix;
  private final File baseDir;
  private UUID randomUUID;
  private ExecutionMode executionMode = ExecutionMode.STANDALONE;
  private String sdcToken;

  public RuntimeInfo(String propertyPrefix, MetricRegistry metrics,
                     List<? extends ClassLoader> stageLibraryClassLoaders) {
    this(propertyPrefix, metrics, stageLibraryClassLoaders, null);
  }
  @VisibleForTesting
  public RuntimeInfo(String propertyPrefix, MetricRegistry metrics,
                     List<? extends ClassLoader> stageLibraryClassLoaders,
                     File baseDir) {
    this.metrics = metrics;
    if(stageLibraryClassLoaders != null) {
      this.stageLibraryClassLoaders = ImmutableList.copyOf(stageLibraryClassLoaders);
    } else {
      this.stageLibraryClassLoaders = null;
    }
    this.propertyPrefix = propertyPrefix;
    this.baseDir = baseDir;
    httpUrl = UNDEF;
    this.attributes = new ConcurrentHashMap<>();
    authenticationTokens = new HashMap<>();
    reloadAuthenticationToken();
    randomUUID = UUID.randomUUID();
  }

  public void init() {
    this.id = getSdcId(getDataDir());
    // inject SDC ID into the API sdc:id EL function
    Utils.setSdcIdCallable(new Callable<String>() {
      @Override
      public String call() throws Exception {
        return RuntimeInfo.this.id;
      }
    });
  }

  protected String getSdcId(String dir) {
    File dataDir = new File(dir);
    if (!dataDir.exists()) {
      if (!dataDir.mkdirs()) {
        throw new RuntimeException(Utils.format("Could not create data directory '{}'", dataDir));
      }
    }
    File idFile = new File(dataDir, "sdc.id");
    if (!idFile.exists()) {
      try {
        Files.write(UUID.randomUUID().toString(), idFile, StandardCharsets.UTF_8);
      } catch (IOException ex) {
        throw new RuntimeException(Utils.format("Could not create SDC ID file '{}': {}", idFile, ex.getMessage(), ex));
      }
    }
    try {
      return Files.readFirstLine(idFile, StandardCharsets.UTF_8).trim();
    } catch (IOException ex) {
      throw new RuntimeException(Utils.format("Could not read SDC ID file '{}': {}", idFile, ex.getMessage(), ex));
    }
  }


  public MetricRegistry getMetrics() {
    return metrics;
  }

  public void setBaseHttpUrl(String url) {
    this.httpUrl = url;
  }

  public String getBaseHttpUrl() {
    return httpUrl;
  }

  public String getId() {
    return id;
  }

  public String getRuntimeDir() {
    if (baseDir != null) {
      return baseDir.getAbsolutePath();
    }
    if (Boolean.getBoolean(TRANSIENT_ENVIRONMENT)) {
      if (Boolean.getBoolean("sdc.testing-mode")) {
        return System.getProperty("user.dir") + "/target/runtime-" + randomUUID;
      } else {
        return System.getProperty("user.dir") + "/" + randomUUID;
      }
    } else {
      return System.getProperty("user.dir");
    }
  }

  public String getStaticWebDir() {
    return System.getProperty(propertyPrefix + STATIC_WEB_DIR, getRuntimeDir() + "/" + propertyPrefix + "-static-web");
  }

  public String getConfigDir() {
    return System.getProperty(propertyPrefix + CONFIG_DIR, getRuntimeDir() + "/etc");
  }

  public String getLogDir() {
    return System.getProperty(propertyPrefix + LOG_DIR, getRuntimeDir() + "/log");
  }

  public String getLog4jPropertiesFileName() {
    return propertyPrefix + LOG4J_PROPERTIES;
  }

  public String getDataDir() {
    return System.getProperty(propertyPrefix + DATA_DIR, getRuntimeDir() + "/var");
  }

  public boolean hasAttribute(String key) {
    Utils.checkNotNull(key, "key");
    return attributes.containsKey(key);
  }

  public <T> void setAttribute(String key, T value) {
    Utils.checkNotNull(key, "key");
    attributes.put(key, value);
  }

  public void removeAttribute(String key) {
    Utils.checkNotNull(key, "key");
    attributes.remove(key);
  }

  @SuppressWarnings("unchecked")
  public <T> T getAttribute(String key) {
    Utils.checkNotNull(key, "key");
    return (T) attributes.get(key);
  }

  public List<? extends ClassLoader> getStageLibraryClassLoaders() {
    return stageLibraryClassLoaders;
  }

  public void log(Logger log) {
    log.info("Runtime info:");
    log.info("  Java version : {}", System.getProperty("java.runtime.version"));
    log.info("  SDC ID       : {}", getId());
    log.info("  Runtime dir  : {}", getRuntimeDir());
    log.info("  Config dir   : {}", getConfigDir());
    log.info("  Data dir     : {}", getDataDir());
    log.info("  Log dir      : {}", getLogDir());
  }

  public void setShutdownHandler(Runnable runnable) {
    shutdownRunnable = runnable;
  }

  public void shutdown() {
    if (shutdownRunnable != null) {
      shutdownRunnable.run();
    }
  }

  public Map<String, String> getAuthenticationTokens() {
    return authenticationTokens;
  }

  public boolean isValidAuthenticationToken(String authToken) {
    String [] authTokens = authToken.split(",");
    for(String token: authTokens) {
      String [] strArr = token.split("\\" + SPLITTER);
      if(strArr.length > 1) {
        String role = strArr[1];
        String tokenCache = authenticationTokens.get(role);
        if(!token.equals(tokenCache)) {
          return false;
        }
      } else {
        return  false;
      }
    }
    return true;
  }

  public String [] getRolesFromAuthenticationToken(String authToken) {
    List<String> roles = new ArrayList<>();
    roles.add(USER_ROLE);

    String [] authTokens = authToken.split(",");
    for(String token: authTokens) {
      String [] strArr = token.split("\\" + SPLITTER);
      if(strArr.length > 1) {
        roles.add(strArr[1]);
      }
    }

    return roles.toArray(new String[roles.size()]);
  }

  public void reloadAuthenticationToken() {
    for(String role: AuthzRole.ALL_ROLES) {
      authenticationTokens.put(role, UUID.randomUUID().toString() + SPLITTER + role);
    }
  }

  public ExecutionMode getExecutionMode() {
    return executionMode;
  }

  public void setExecutionMode(String executionMode) {
    try {
      this.executionMode = ExecutionMode.valueOf(executionMode.trim().toUpperCase());
    } catch (IllegalArgumentException ex) {
      this.executionMode = ExecutionMode.STANDALONE;
    }
  }

  public void setSDCToken(String sdcToken) {
    this.sdcToken = sdcToken;
  }

  public String getSDCToken() {
    return sdcToken;
  }

  public String getClusterCallbackURL() {
    return getBaseHttpUrl() + CALLBACK_URL;
  }
}
