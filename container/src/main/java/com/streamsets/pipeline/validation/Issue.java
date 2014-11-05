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
package com.streamsets.pipeline.validation;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

public class Issue {

  public interface ResourceBundleProvider {
    public ResourceBundle get();
  }

  private final String bundleKey;
  private final String defaultTemplate;
  private final Object[] args;
  private ResourceBundleProvider resourceBundleProvider;

  public Issue(String bundleKey, String defaultTemplate, Object... args) {
    this.bundleKey = Preconditions.checkNotNull(bundleKey, "bundleKey cannot be null");
    this.defaultTemplate = Preconditions.checkNotNull(defaultTemplate, "defaultTemplate cannot be null");
    this.args = args;
  }

  public void setResourceBundleProvider(ResourceBundleProvider resourceBundleProvider) {
    this.resourceBundleProvider = resourceBundleProvider;
  }

  public String getMessage() {
    ResourceBundle rb = (resourceBundleProvider != null) ? resourceBundleProvider.get() : null;
    String template = defaultTemplate;
    if (rb != null && rb.containsKey(bundleKey)) {
      template = rb.getString(bundleKey);
    }
    return String.format(template, args);
  }

}
