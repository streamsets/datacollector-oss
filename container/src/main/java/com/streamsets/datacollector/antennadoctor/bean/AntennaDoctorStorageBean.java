/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.datacollector.antennadoctor.bean;

import java.util.List;

/**
 * Main storage bean for Antenna Doctor regardless whether this is stored copy inside jar classpath or expanded
 * copy in the data directory. This file is meant to contain all active rules regardless whether they are applicable
 * to this release and runtime of data collector.
 */
public class AntennaDoctorStorageBean {

  /**
   * Current version (of the code) for the schema version.
   */
  public final static int CURRENT_SCHEMA_VERSION = 1;

  /**
   * Version of schema of this file.
   *
   * Primarily used to determine version of the structure so that we can perform upgrades if/when necessary.
   */
  private int schemaVersion;

  /**
   * Base version of the storage.
   *
   * Format is YYYYMMDDHHMMSS (e.g. string version of timestamp).
   */
  private String baseVersion;

  /**
   * Updates that were already applied to this base version.
   */
  private List<String> updates;

  /**
   * All rules for the Antenna Doctor engine.
   */
  private List<AntennaDoctorRuleBean> rules;

  public int getSchemaVersion() {
    return schemaVersion;
  }

  public void setSchemaVersion(int schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  public String getBaseVersion() {
    return baseVersion;
  }

  public void setBaseVersion(String baseVersion) {
    this.baseVersion = baseVersion;
  }

  public List<String> getUpdates() {
    return updates;
  }

  public void setUpdates(List<String> updates) {
    this.updates = updates;
  }

  public List<AntennaDoctorRuleBean> getRules() {
    return rules;
  }

  public void setRules(List<AntennaDoctorRuleBean> rules) {
    this.rules = rules;
  }
}
