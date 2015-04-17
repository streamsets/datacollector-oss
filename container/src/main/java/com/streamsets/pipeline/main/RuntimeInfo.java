/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.main;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class RuntimeInfo {
  public static final String SPLITTER = "|";
  public static final String CONFIG_DIR = "sdc.conf.dir";
  public static final String DATA_DIR = "sdc.data.dir";
  public static final String LOG_DIR = "sdc.log.dir";
  public static final String STATIC_WEB_DIR = "sdc.static-web.dir";

  public static final String LOG4J_CONFIGURATION_URL_ATTR = "log4j.configuration.url";
  private final MetricRegistry metrics;
  private final List<? extends ClassLoader> stageLibraryClassLoaders;
  private String id;
  private String httpUrl;
  private final Map<String, Object> attributes;
  private Runnable shutdownRunnable;
  private Map<String, String> authenticationTokens;
  private final String [] roles = { "admin", "creator", "manager", "guest" };

  public RuntimeInfo(MetricRegistry metrics, List<? extends ClassLoader> stageLibraryClassLoaders) {
    this.metrics = metrics;
    this.stageLibraryClassLoaders = ImmutableList.copyOf(stageLibraryClassLoaders);
    id = "UNDEF";
    httpUrl = "UNDEF";
    this.attributes = new ConcurrentHashMap<>();
    authenticationTokens = new HashMap<>();
    reloadAuthenticationToken();
  }

  public MetricRegistry getMetrics() {
    return metrics;
  }

  public void setId(String id) {
    this.id = id;
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
    return System.getProperty("user.dir");
  }

  public String getStaticWebDir() {
    return System.getProperty(STATIC_WEB_DIR, getRuntimeDir() + "/static-web");
  }

  public String getConfigDir() {
    return System.getProperty(CONFIG_DIR, getRuntimeDir() + "/etc");
  }

  public String getLogDir() {
    return System.getProperty(LOG_DIR, getRuntimeDir() + "/log");
  }

  public String getDataDir() {
    return System.getProperty(DATA_DIR, getRuntimeDir() + "/var");
  }

  public boolean hasAttribute(String key) {
    Utils.checkNotNull(key, "key");
    return attributes.containsKey(key);
  }

  public <T> void setAttribute(String key, T value) {
    Utils.checkNotNull(key, "key");
    attributes.put(key, value);
  }

  public <T> void removeAttribute(String key) {
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
    String [] strArr = authToken.split(SPLITTER);
    if(strArr.length > 1) {
      String role = strArr[1];
      String tokenCache = authenticationTokens.get(role);
      return authToken.equals(tokenCache);
    }
    return false;
  }

  public String getRoleFromAuthenticationToken(String authToken) {
    String [] strArr = authToken.split(SPLITTER);
    if(strArr.length > 1) {
      return strArr[1];
    }

    return null;
  }

  public void reloadAuthenticationToken() {
    for(String role: roles) {
      authenticationTokens.put(role, UUID.randomUUID().toString() + SPLITTER + role);
    }
  }
}
