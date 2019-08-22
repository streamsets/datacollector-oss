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
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.api.Label;

public enum KeyCaptureMode implements Label {
  NONE("None"),
  RECORD_HEADER("Record Header"),
  RECORD_FIELD("Record Field"),
  RECORD_HEADER_AND_FIELD("Record Header and Field"),
  ;

  private final String label;

  KeyCaptureMode(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
