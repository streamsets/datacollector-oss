/**
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
package com.streamsets.datacollector.main;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.pipeline.api.impl.Utils;

import org.slf4j.Logger;

import javax.net.ssl.SSLContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


public abstract class RuntimeInfo {
  public static final String SPLITTER = "|";
  public static final String CONFIG_DIR = ".conf.dir";
  public static final String DATA_DIR = ".data.dir";
  public static final String LOG_DIR = ".log.dir";
  public static final String RESOURCES_DIR = ".resources.dir";
  public static final String LIBEXEC_DIR = ".libexec.dir";
  public static final String STATIC_WEB_DIR = ".static-web.dir";
  public static final String TRANSIENT_ENVIRONMENT = "sdc.transient-env";
  public static final String UNDEF = "UNDEF";
  public static final String CALLBACK_URL = "/public-rest/v1/cluster/callback";
  private boolean DPMEnabled;
  private boolean aclEnabled;

  private final static String USER_ROLE = "user";

  public static final String LOG4J_CONFIGURATION_URL_ATTR = "log4j.configuration.url";
  public static final String LOG4J_PROPERTIES = "-log4j.properties";

  private static final String STREAMSETS_LIBRARIES_EXTRA_DIR_SYS_PROP = "STREAMSETS_LIBRARIES_EXTRA_DIR";

  private final MetricRegistry metrics;
  private final List<? extends ClassLoader> stageLibraryClassLoaders;
  private String httpUrl;
  private String appAuthToken;
  private final Map<String, Object> attributes;
  private ShutdownHandler shutdownRunnable;
  private final Map<String, String> authenticationTokens;
  private final String propertyPrefix;
  private final UUID randomUUID;
  private SSLContext sslContext;
  private boolean remoteRegistrationSuccessful;

  public RuntimeInfo(String propertyPrefix, MetricRegistry metrics,
                     List<? extends ClassLoader> stageLibraryClassLoaders) {
    this.metrics = metrics;
    if(stageLibraryClassLoaders != null) {
      this.stageLibraryClassLoaders = ImmutableList.copyOf(stageLibraryClassLoaders);
    } else {
      this.stageLibraryClassLoaders = null;
    }
    this.propertyPrefix = propertyPrefix;
    httpUrl = UNDEF;
    this.attributes = new ConcurrentHashMap<>();
    authenticationTokens = new HashMap<>();
    reloadAuthenticationToken();
    randomUUID = UUID.randomUUID();
  }

  protected UUID getRandomUUID() {
    return randomUUID;
  }

  public abstract void init();

  public abstract String getId();

  public abstract String getMasterSDCId();

  public abstract String getRuntimeDir();

  public abstract boolean isClusterSlave();

  public MetricRegistry getMetrics() {
    return metrics;
  }

  public void setBaseHttpUrl(String url) {
    this.httpUrl = url;
  }

  public String getBaseHttpUrl() {
    return httpUrl;
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

  public String getLibexecDir() {
    return System.getProperty(propertyPrefix + LIBEXEC_DIR, getRuntimeDir() + "/libexec");
  }

  public String getResourcesDir() {
    return System.getProperty(propertyPrefix + RESOURCES_DIR, getRuntimeDir() + "/resources");
  }

  public String getLibsExtraDir() {
    return System.getProperty(STREAMSETS_LIBRARIES_EXTRA_DIR_SYS_PROP, null);
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

  public void setShutdownHandler(ShutdownHandler runnable) {
    shutdownRunnable = runnable;
  }

  public void shutdown(int status) {
    if (shutdownRunnable != null) {
      shutdownRunnable.setExistStatus(status);
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

  public String getClusterCallbackURL() {
    return getBaseHttpUrl() + CALLBACK_URL;
  }

  public void setRemoteRegistrationStatus(boolean remoteRegistrationSuccessful) {
    this.remoteRegistrationSuccessful = remoteRegistrationSuccessful;
  }

  public boolean isRemoteRegistrationSuccessful() {
    return this.remoteRegistrationSuccessful;
  }

  public void setSSLContext(SSLContext sslContext) {
    this.sslContext = sslContext;
  }

  public SSLContext getSSLContext() {
    return sslContext;
  }

  void setAppAuthToken(String appAuthToken) {
    this.appAuthToken = appAuthToken;
  }

  public String getAppAuthToken() {
    return appAuthToken;
  }

  public void setDPMEnabled(boolean DPMEnabled) {
    this.DPMEnabled = DPMEnabled;
  }

  public boolean isDPMEnabled() {
    return DPMEnabled;
  }

  public boolean isAclEnabled() {
    return aclEnabled;
  }

  public void setAclEnabled(boolean aclEnabled) {
    this.aclEnabled = aclEnabled;
  }
}
