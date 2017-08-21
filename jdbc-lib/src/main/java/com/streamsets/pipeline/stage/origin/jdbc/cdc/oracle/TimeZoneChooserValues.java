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

package com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle;

import com.streamsets.pipeline.api.ChooserValues;
import org.jetbrains.annotations.NotNull;

import java.time.ZoneId;
import java.time.format.TextStyle;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

// This is a copy-paste of the globally available one but migrated to the new Java 8 time API since that is the one
// the origin uses, and so we add "same as datacollector" as the first option.
public class TimeZoneChooserValues implements ChooserValues {
  private static final List<String> VALUES = new ArrayList<>();
  private static final List<String> LABELS = new ArrayList<>();

  static {
    ZoneId systemDefault = ZoneId.systemDefault();
    VALUES.add(systemDefault.getId());
    LABELS.add("Same as Data Collector: " + getLabelForTimeZoneId(systemDefault));
    for (String tzId : ZoneId.getAvailableZoneIds()) {
      // skip id's that are like "Etc/GMT+01:00" because their display names are like "GMT-01:00", which is confusing
      if (!tzId.startsWith("Etc/GMT")) {
        ZoneId zoneId = ZoneId.of(tzId);
        if (!zoneId.equals(systemDefault)) {
          VALUES.add(tzId);
          LABELS.add(getLabelForTimeZoneId(zoneId));
        }
      }
    }
  }

  @NotNull
  private static String getLabelForTimeZoneId(ZoneId zoneId) {
    return zoneId.getDisplayName(TextStyle.SHORT, Locale.getDefault()) + " (" + zoneId.getId() + ")";
  }

  @Override
  public String getResourceBundle() {
    return null;
  }

  @Override
  public List<String> getValues() {
    return VALUES;
  }

  @Override
  public List<String> getLabels() {
    return LABELS;
  }
}
