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
package com.streamsets.pipeline.runner;

import com.fasterxml.jackson.annotation.JsonValue;
import com.streamsets.pipeline.validation.Issue;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

public class ErrorRecords {
  private static final String PIPELINE_CONTAINER_BUNDLE = "pipeline-container-bundle";

  private final List<ErrorRecord> errors;
  private final Issue.ResourceBundleProvider resourceBundleProvider;
  private ResourceBundle resourceBundle;

  public ErrorRecords() {
    errors = new ArrayList<ErrorRecord>();
    resourceBundleProvider = new Issue.ResourceBundleProvider() {
      @Override
      public ResourceBundle get() {
        return resourceBundle;
      }
    };
  }

  public void setLocale(Locale locale) {
    locale = (locale != null) ? locale : Locale.getDefault();
    resourceBundle = ResourceBundle.getBundle(PIPELINE_CONTAINER_BUNDLE, locale);
  }

  public void addErrorRecord(ErrorRecord errorRecord) {
    errorRecord.getIssue().setResourceBundleProvider(resourceBundleProvider);
    errors.add(errorRecord);
  }

  @JsonValue
  public List<ErrorRecord> getErrorRecords() {
    return errors;
  }
}
