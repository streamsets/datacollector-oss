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

import com.streamsets.pipeline.api.ChooserValues;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.TextStyle;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class TimeZoneChooserValues implements ChooserValues {
  private static final List<String> VALUES = new ArrayList<>();
  private static final List<String> LABELS = new ArrayList<>();

  static {
    ZoneId systemDefault = ZoneId.systemDefault();
    VALUES.add(systemDefault.getId());
    LABELS.add("Same as Data Collector: " + getLabelForTimeZoneId(systemDefault));

    Map<String, String> zones = new HashMap<>();
    for (String tzId : ZoneId.getAvailableZoneIds()) {
      // skip id's that are like "Etc/GMT+01:00" because their display names are like "GMT-01:00", which is confusing
      if (!tzId.startsWith("Etc/GMT")) {
        ZoneId zoneId = ZoneId.of(tzId);
        if (!zoneId.equals(systemDefault)) {
          zones.put(getLabelForTimeZoneId(zoneId), tzId);
        }
      }
    }

    zones.entrySet().stream()
      .sorted(Map.Entry.comparingByKey(new TimezoneLabelComparator()))
      .forEach(entry -> {
        LABELS.add(entry.getKey());
        VALUES.add(entry.getValue());
      });
  }

  /**
   * Create user visible label for given zone id.
   *
   * The created labels are in format $OFFSET $SHORT ($NAME), e.g.
   *
   * +01:00 CET (Europe/Prague)
   * -07:00 PST (America/Los_Angeles)
   */
//  @VisibleForTesting
  static String getLabelForTimeZoneId(ZoneId zoneId) {
    ZoneOffset zos = LocalDateTime.now().atZone(zoneId).getOffset();

    return zos.getId().replaceAll("Z", "+00:00") + " " + zoneId.getDisplayName(TextStyle.SHORT, Locale.getDefault()) + " (" + zoneId.getId() + ")";
  }

  /**
   * This is private comparator that is expected to be used on our labels that always starts with
   * timezone offset. E.g. '+01:00 CET ...'.
   *
   * We sort such strings from WEST to EAST, e.g.:
   *
   * -11:00
   * -01:00
   * +01:00
   * +11:00
   *
   */
  private static class TimezoneLabelComparator implements Comparator<String> {
    @Override
    public int compare(String a, String b) {
      // If both are positive then we can simply compare the string directly ('+11' is more then '+01')
      if(a.startsWith("+") && b.startsWith("+")) {
        return a.compareTo(b);
      // If both are negative we need to reverse the comparison ('-11' should be less then '-01')
      } else if(a.startsWith("-") && b.startsWith("-")) {
        return -a.compareTo(b);
      // If the signs differ then '-' will be lower
      } else {
        return a.startsWith("-") ? -1 : 1;
      }
    }
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
