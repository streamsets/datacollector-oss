/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

public class Issue {
  private static final String PIPELINE_CONTAINER_BUNDLE = "pipeline-container-bundle";

  private final String component;
  private final String bundleKey;
  private final String defaultTemplate;
  private final Object[] args;

  public Issue(String component, String bundleKey, String defaultTemplate, Object... args) {
    this.component = component;
    this.bundleKey = bundleKey;
    this.defaultTemplate = defaultTemplate;
    this.args = args;
  }

  private String getMessage(String template) {
    return String.format(template, args);
  }

  public String getMessage() {
    return getMessage(defaultTemplate);
  }

  public static Map<String, List<String>> getLocalizedMessages(List<Issue> issues, Locale locale) {
    Map<String, List<String>> map = new LinkedHashMap<String, List<String>>();
    ResourceBundle rb = ResourceBundle.getBundle(PIPELINE_CONTAINER_BUNDLE, locale);
    for (Issue issue : issues) {
      List<String> list = map.get(issue.component);
      if (list == null) {
        list = new ArrayList<String>(issues.size());
        map.put(issue.component, list);
      }
      String template = (rb.containsKey(issue.bundleKey)) ? rb.getString(issue.bundleKey) : issue.defaultTemplate;
      list.add(issue.getMessage(template));
    }
    return map;
  }

}
