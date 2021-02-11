/*
 * Copyright 2021 StreamSets Inc.
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

@GenerateResourceBundle
public enum CsvParser implements Label {
  // Use the Univocity parser - faster, more feature rich (have support for multi-character delimiters, ...)
  UNIVOCITY("Univocity - faster but less powerful"),
  // Legacy parsing logic, it's actually not a single parser, but rather two depending on subsequent options and their
  // combination user chooses. Complicated and sadly not that clean.
  LEGACY_PARSER("Apache Commons - slower but more powerful"),
  ;

  private final String label;

  CsvParser(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
