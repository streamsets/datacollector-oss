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
package com.streamsets.pipeline.lib.jdbc;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum DataType implements Label {
  USE_COLUMN_TYPE("Use db column type"),
  BOOLEAN("BOOLEAN"),
  SHORT("SHORT"),
  INTEGER("INTEGER"),
  LONG("LONG"),
  FLOAT("FLOAT"),
  DOUBLE("DOUBLE"),
  DATE("DATE"),
  DATETIME("DATETIME"),
  TIME("TIME"),
  DECIMAL("DECIMAL"),
  STRING("STRING"),
  BYTE_ARRAY("BYTE_ARRAY");

  private String label;

  DataType(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
