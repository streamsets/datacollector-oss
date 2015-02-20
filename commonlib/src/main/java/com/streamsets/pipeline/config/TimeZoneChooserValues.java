/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.ChooserValues;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

public class TimeZoneChooserValues implements ChooserValues {
  private static final List<String> VALUES = new ArrayList<String>();
  private static final List<String> LABELS = new ArrayList<String>();

  static {
    Set<String> seen = new HashSet<>();
    for (String tzId : TimeZone.getAvailableIDs()) {
      // skip id's that are like "Etc/GMT+01:00" because their display names are like "GMT-01:00", which is confusing
      if (!tzId.startsWith("Etc/GMT")) {
        if (!seen.contains(tzId)) {
          TimeZone tZone = TimeZone.getTimeZone(tzId);
          VALUES.add(tzId);
          LABELS.add(tZone.getDisplayName(false, TimeZone.SHORT) + " (" + tzId + ")");
          seen.add(tzId);
        }
      }
    }
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
