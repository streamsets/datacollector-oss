/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class BuildInfo {
  private static final Logger LOG = LoggerFactory.getLogger(BuildInfo.class);
  private static final String API_BUILD_INFO_FILE = "pipeline-api-build-info.properties";
  private static final String CONTAINER_BUILD_INFO_FILE = "pipeline-container-build-info.properties";

  private final Properties apiInfo;
  private final Properties implInfo;

  private Properties load(String resourceName) {
    Properties props = new Properties();
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    InputStream is = cl.getResourceAsStream(resourceName);
    if (is != null) {
      try {
        props.load(is);
        is.close();
      } catch (Exception ex) {
        LOG.error("Could not read '{}' from classpath: {}", resourceName, ex.toString(), ex);
      }
    }
    return props;
  }

  public BuildInfo() {
    apiInfo = load(API_BUILD_INFO_FILE);
    implInfo = load(CONTAINER_BUILD_INFO_FILE);
  }

  public String getVersion() {
    return implInfo.getProperty("pipeline-container.version", "?");
  }

  public String getBuiltDate() {
    return implInfo.getProperty("pipeline-container.built.date", "?");
  }

  public String getBuiltBy() {
    return implInfo.getProperty("pipeline-container.built.by", "?");
  }

  public String getBuiltRepoSha() {
    return implInfo.getProperty("pipeline-container.built.repo.sha", "?");
  }

  public String getApiSourceMd5Checksum() {
    return apiInfo.getProperty("pipeline-api.source.md5.checksum", "?");
  }

  public String getImplSourceMd5Checksum() {
    return implInfo.getProperty("pipeline-container.source.md5.checksum", "?");
  }

  public void log(Logger log) {
    log.info("Build info:");
    log.info("  Version        : {}", getVersion());
    log.info("  Date           : {}", getBuiltDate());
    log.info("  Built by       : {}", getBuiltBy());
    log.info("  Repo SHA       : {}", getBuiltRepoSha());
    log.info("  API Source MD5 : {}", getApiSourceMd5Checksum());
    log.info("  Impl Source MD5: {}", getImplSourceMd5Checksum());
    String apiRepoSha = apiInfo.getProperty("pipeline-api.built.repo.sha", "?");
    if (!getBuiltRepoSha().equals(apiRepoSha)) {
      log.error("  It seems there is a mismatch between the API & Impl builds");
      log.error("    API Version  : {}", apiInfo.getProperty("pipeline-api.version", "?"));
      log.error("    API Date     : {}", apiInfo.getProperty("pipeline-api.built.date", "?"));
      log.error("    API Built by : {}", apiInfo.getProperty("pipeline-api.built.by", "?"));
      log.error("    API Repo SHA : {}", apiRepoSha);
    }
  }

}
