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
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class LocaleChooserValues implements ChooserValues {
  private static final List<String> VALUES = new ArrayList<String>();
  private static final List<String> LABELS = new ArrayList<String>();

  static {
    Set<String> seen = new HashSet<>();
    Map<String, String> labelToValue = new HashMap<>();
    for (Locale locale : Locale.getAvailableLocales()) {
      StringBuilder sb = new StringBuilder();
      if (!locale.getLanguage().isEmpty()) {
        sb.append(locale.getLanguage());
      }
      if (!locale.getCountry().isEmpty()) {
        sb.append(",").append(locale.getCountry());
      }
      if (!locale.getVariant().isEmpty()) {
        sb.append(",").append(locale.getVariant());
      }
      String key = sb.toString();
      if (!key.isEmpty() && !seen.contains(key)) {
        labelToValue.put(locale.getDisplayName(), key);
        seen.add(key);
      }
    }
    // sort display names alphabetically.
    List<String> names = new ArrayList<>(labelToValue.keySet());
    Collections.sort(names);
    for (String name : names) {
      String key = labelToValue.get(name);
      VALUES.add(key);
      LABELS.add(name);
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

  public static Locale getLocale(String key) {
    Utils.checkNotNull(key, "key");
    String split[] = key.split(",", 3);
    switch (split.length) {
      case 1:
        return new Locale(split[0]);
      case 2:
        return new Locale(split[0], split[1]);
      case 3:
        return new Locale(split[0], split[1], split[2]);
      default:
        throw new IllegalArgumentException(Utils.format("Invalid Locale '{}'", key));
    }
  }

}
