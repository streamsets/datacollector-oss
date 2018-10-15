/*
 * Copyright 2018 StreamSets Inc.
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

import com.streamsets.pipeline.api.Label;

/** List of actions for duplicate-key errors */
public enum DuplicateKeyAction implements Label {
  IGNORE("Ignore", "IGNORE"),
  REPLACE("Replace", "REPLACE"),
  ;

  /** UI label */
  private final String label;

  /** Keyword for query statement */
  private final String keyword;

  DuplicateKeyAction(String label, String keyword) {
    this.label = label;
    this.keyword = keyword;
  }

  @Override
  public String getLabel() {
    return label;
  }

  /** @return the keyword for query statement */
  public String getKeyword() {
    return keyword;
  }
}
