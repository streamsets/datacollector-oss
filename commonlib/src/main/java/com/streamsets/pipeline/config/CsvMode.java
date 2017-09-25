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
import org.apache.commons.csv.CSVFormat;

@GenerateResourceBundle
public enum CsvMode implements Label {
  CSV("Default CSV (ignores empty lines)", CSVFormat.DEFAULT),
  RFC4180("RFC4180 CSV", CSVFormat.RFC4180),
  EXCEL("MS Excel CSV", CSVFormat.EXCEL),
  MYSQL("MySQL CSV", CSVFormat.MYSQL),
  TDF("Tab Separated Values", CSVFormat.TDF),
  POSTGRES_CSV("Postgres CSV", CSVFormat.POSTGRESQL_CSV),
  POSTGRES_TEXT("Postgres Text", CSVFormat.POSTGRESQL_TEXT),
  CUSTOM("Custom", null)
  ;

  private final String label;
  private final CSVFormat format;

  CsvMode(String label, CSVFormat format) {
    this.label = label;
    this.format = format;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public CSVFormat getFormat() {
    return format;
  }

}
