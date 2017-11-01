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
package com.streamsets.pipeline.lib.http.logging;

import com.streamsets.pipeline.api.ChooserValues;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

public class JulLogLevelChooserValues implements ChooserValues {

  public static final String DEFAULT_LEVEL = "FINE";

  private final List<String> values;
  private final List<String> labels;

  {
    final List<String> allValues = new LinkedList<>();
    final List<String> allLabels = new LinkedList<>();

    for (Level level : new Level[] {
        Level.OFF,
        Level.SEVERE,
        Level.WARNING,
        Level.INFO,
        Level.CONFIG,
        Level.FINE,
        Level.FINER,
        Level.FINEST,
        Level.ALL
    }) {
      allValues.add(level.getName());
      allLabels.add(level.getLocalizedName());
    }

    values = Collections.unmodifiableList(allValues);
    labels = Collections.unmodifiableList(allLabels);
  }

  @Override
  public String getResourceBundle() {
    return null;
  }

  @Override
  public List<String> getValues() {
    return values;
  }

  @Override
  public List<String> getLabels() {
    return labels;
  }
}
