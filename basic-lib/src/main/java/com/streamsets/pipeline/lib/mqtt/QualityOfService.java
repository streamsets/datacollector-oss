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
package com.streamsets.pipeline.lib.mqtt;

import com.streamsets.pipeline.api.Label;

public enum QualityOfService implements Label {
  AT_MOST_ONCE("At Most Once (0)", 0),
  AT_LEAST_ONCE("At Least Once (1)", 1),
  EXACTLY_ONCE("Exactly Once (2)", 2);

  private final String label;
  private final int value;

  QualityOfService(String label, int value) {
    this.label = label;
    this.value = value;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public int getValue() {
    return value;
  }

}
