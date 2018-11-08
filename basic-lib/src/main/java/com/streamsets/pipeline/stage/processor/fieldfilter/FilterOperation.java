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
package com.streamsets.pipeline.stage.processor.fieldfilter;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum FilterOperation implements Label {
  KEEP("Keep Listed Fields"),
  REMOVE("Remove Listed Fields"),
  REMOVE_NULL("Remove Listed Fields If Their Values Are Null"),
  REMOVE_EMPTY("Remove Listed Fields If Their Values Are Empty String"),
  REMOVE_NULL_EMPTY("Remove Listed Fields If Their Values Are Null or Empty String"),
  REMOVE_CONSTANT("Remove Listed Fields If Their Values Are The Given Constant");

  private String label;

  private FilterOperation(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
