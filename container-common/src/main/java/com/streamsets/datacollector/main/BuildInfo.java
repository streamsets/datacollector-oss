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
package com.streamsets.datacollector.main;

import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;

import javax.annotation.concurrent.ThreadSafe;
import java.io.InputStream;
import java.util.Properties;

@ThreadSafe
public abstract class BuildInfo {
  private final Properties info;

  protected BuildInfo(String buildInfoFile) {
    info = load(buildInfoFile);
  }

  protected Properties load(String resourceName) {
    Properties props = new Properties();
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    InputStream is = cl.getResourceAsStream(resourceName);
    if (is != null) {
      try {
        props.load(is);
        is.close();
      } catch (Exception ex) {
        throw new RuntimeException(
            Utils.format("Could not read '{}' from classpath: {}", resourceName, ex.toString()),
            ex
        );
      }
    }
    return props;
  }

  public Properties getInfo() {
    // return new instance to avoid possibility of modification
    final Properties propertiesClone = new Properties();
    propertiesClone.putAll(this.info);
    return propertiesClone;
  }

  public String getVersion() {
    return info.getProperty("version", "?");
  }

  public String getBuiltDate() {
    return info.getProperty("built.date", "?");
  }

  public String getBuiltBy() {
    return info.getProperty("built.by", "?");
  }

  public String getBuiltRepoSha() {
    return info.getProperty("built.repo.sha", "?");
  }

  public String getSourceMd5Checksum() {
    return info.getProperty("source.md5.checksum", "?");
  }

  public String getScalaBinaryVersion() {
    return info.getProperty("scala.binary.version", "?");
  }

  public void log(Logger log) {
    log.info("Build info:");
    log.info("  Version        : {}", getVersion());
    log.info("  Date           : {}", getBuiltDate());
    log.info("  Built by       : {}", getBuiltBy());
    log.info("  Repo SHA       : {}", getBuiltRepoSha());
    log.info("  Source MD5     : {}", getSourceMd5Checksum());
    if (!getScalaBinaryVersion().equals("?")) {
      // For Transformer
      log.info("  Scala Binary Version     : {}", getScalaBinaryVersion());
    }
  }

}
