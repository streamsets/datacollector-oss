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

import com.streamsets.pipeline.api.Label;

import java.time.format.DateTimeFormatter;
import java.util.Optional;

public enum ZonedDateTimeFormat implements Label {

  // We support only masks that actually have offsets/timezones - because that is what makes the most sense.
  // For other masks which don't have offsets or timezones, the user can specify it directly.
  ISO_OFFSET_DATE_TIME("yyyy-MM-dd'T'HH:mm:ssX"),
  ISO_ZONED_DATE_TIME("yyyy-MM-dd'T'HH:mm:ssX[VV]"),
  OTHER("Other ...")
  ;
  private final String label;

  private DateTimeFormatter formatter;

  ZonedDateTimeFormat(String label) {
    this.label = label;
    switch (label) {
      case "yyyy-MM-dd'T'HH:mm:ssX":
        formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
        break;
      case "yyyy-MM-dd'T'HH:mm:ssX[VV]":
        formatter = DateTimeFormatter.ISO_ZONED_DATE_TIME;
        break;
      default:
        formatter = null;
    }
  }

  @Override
  public String getLabel() {
    return label;
  }

  public Optional<DateTimeFormatter> getFormatter() {
    return Optional.ofNullable(formatter);
  }
}
