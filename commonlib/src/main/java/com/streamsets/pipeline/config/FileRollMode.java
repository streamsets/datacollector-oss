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
import com.streamsets.pipeline.lib.io.LogRollModeFactory;
import com.streamsets.pipeline.lib.io.PeriodicFilesRollModeFactory;
import com.streamsets.pipeline.lib.io.RollMode;
import com.streamsets.pipeline.lib.io.RollModeFactory;

@GenerateResourceBundle
public enum FileRollMode implements Label {
  REVERSE_COUNTER("Active File with Reverse Counter Files", LogRollModeFactory.REVERSE_COUNTER),
  DATE_YYYY_MM("Active File with .yyyy-MM Files", LogRollModeFactory.DATE_YYYY_MM),
  DATE_YYYY_MM_DD("Active File with .yyyy-MM-dd Files", LogRollModeFactory.DATE_YYYY_MM_DD),
  DATE_YYYY_MM_DD_HH("Active File with .yyyy-MM-dd-HH Files", LogRollModeFactory.DATE_YYYY_MM_DD_HH),
  DATE_YYYY_MM_DD_HH_MM("Active File with .yyyy-MM-dd-HH-mm Files", LogRollModeFactory.DATE_YYYY_MM_DD_HH_MM),
  DATE_YYYY_WW("Active File with .yyyy-ww Files", LogRollModeFactory.DATE_YYYY_WW),
  ALPHABETICAL("Active File with Alphabetical Files", LogRollModeFactory.ALPHABETICAL),
  PATTERN("Files matching a pattern", new PeriodicFilesRollModeFactory()),

  ;

  private final String label;
  private final RollModeFactory factory;

  FileRollMode(String label, RollModeFactory factory) {
    this.label = label;
    this.factory = factory;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public RollMode createRollMode(String fileName, String pattern) {
    return factory.get(fileName, pattern);
  }

  public String getTokenForPattern() {
    return factory.getTokenForPattern();
  }

}
