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
 * Message bean that should be displayed to user if the associated rule matches.
 */
public class AntennaDoctorMessageBean {

  /**
   * Smaller summary of the issue.
   *
   * Can be displayed in smaller space widgets such as tooltips. Must be plain text (no HTML allowed).
   */
  private String summary;

  /**
   * Longer description of the issue with suggestion of a remedy.
   *
   * Will always be displayed in larger space widget. Can contain rich formatting using HTML.
   *
   * We define the description as list of strings, albeit internally we concatenate it using new line characters. This is
   * so that manual edit of the policy file is easier - one can have multiple lines separately since JSON does not support
   * multi-line strings.
   */
  private List<String> description;

  public String getSummary() {
    return summary;
  }

  public void setSummary(String summary) {
    this.summary = summary;
  }

  public List<String> getDescription() {
    return description;
  }

  public void setDescription(List<String> description) {
    this.description = description;
  }
}
