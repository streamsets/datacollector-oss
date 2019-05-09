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
 * Antenna Doctor uses remote repository in order to be able to download updated knowledge base.
 *
 * Repository structure is 'simple':
 *
 * * manifest.json: This bean. Unzipped.
 * * base-YYYYMMDDHHMMSS.json.zip: AntennaDoctorStorageBean with base file.
 * * update-YYYYMMDDHHMMSS.json.zip: AntennaDoctorUpdateBean
 */
public class AntennaDoctorRepositoryManifestBean {

  /**
   * Current version (of the code) for the schema version.
   */
  public final static int CURRENT_SCHEMA_VERSION = 1;

  /**
   * Schema version of the manifest file.
   */
  private int schemaVersion;

  /**
   * Base version of the storage.
   *
   * Format is YYYYMMDDHHMMSS (e.g. string version of timestamp).
   */
  private String baseVersion;

  /**
   * Ordered list of versions that should be applied on top of the base version.
   */
  private List<String> updates;

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
}
