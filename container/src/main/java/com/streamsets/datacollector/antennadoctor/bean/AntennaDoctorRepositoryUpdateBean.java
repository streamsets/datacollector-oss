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
 * Bean describing updates to the base file.
 *
 * Version of the update is implied by the filename of this file and is not loaded from the file itself.
 */
public class AntennaDoctorRepositoryUpdateBean {

  /**
   * Rules that should be added or updated.
   */
  private List<AntennaDoctorRuleBean> updates;

  /**
   * List of rule UUIDs that should be removed.
   */
  private List<String> deletes;

  public List<AntennaDoctorRuleBean> getUpdates() {
    return updates;
  }

  public void setUpdates(List<AntennaDoctorRuleBean> updates) {
    this.updates = updates;
  }

  public List<String> getDeletes() {
    return deletes;
  }

  public void setDeletes(List<String> deletes) {
    this.deletes = deletes;
  }
}
