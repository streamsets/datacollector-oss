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
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.ext.json.Mode;

@GenerateResourceBundle
public enum JsonMode implements Label {
  ARRAY_OBJECTS("JSON array of objects", Mode.ARRAY_OBJECTS),
  MULTIPLE_OBJECTS("Multiple JSON objects", Mode.MULTIPLE_OBJECTS),
  ;

  private final String label;
  private final Mode format;

  JsonMode(String label, Mode format) {
    this.label = label;
    this.format = format;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public Mode getFormat() {
    return format;
  }

}
