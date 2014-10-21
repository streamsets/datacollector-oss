/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class BuildInfo {
  private static final Logger LOG = LoggerFactory.getLogger(BuildInfo.class);
  private static final String BUILD_INFO_FILE = "pipeline-build-info.properties";

  private final Properties info;

  public BuildInfo() {
    info = new Properties();
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    InputStream is = cl.getResourceAsStream(BUILD_INFO_FILE);
    if (is != null) {
      try {
        info.load(is);
        is.close();
      } catch (Exception ex) {
        LOG.error("Could not read '{}' from classpath: {}", BUILD_INFO_FILE, ex.toString(), ex);
      }
    }
  }

  public String getVersion() {
    return info.getProperty("pipeline.version", "?");
  }

  public String getBuiltDate() {
    return info.getProperty("pipeline.built.date", "?");
  }

  public String getBuiltBy() {
    return info.getProperty("pipeline.built.by", "?");
  }

  public String getBuiltRepoSha() {
    return info.getProperty("pipeline.built.repo.sha", "?");
  }

  public String getSourceMd5Checksum() {
    return info.getProperty("pipeline.built.source.md5.checksum", "?");
  }

  public void log(Logger log) {
    log.info("Build info:");
    log.info("  Version         : {}", getVersion());
    log.info("  Built date      : {}", getBuiltDate());
    log.info("  Built by        : {}", getBuiltBy());
    log.info("  Built Repo SHA  : {}", getBuiltRepoSha());
    log.info("  Built Source MD5: {}", getSourceMd5Checksum());
  }

}
