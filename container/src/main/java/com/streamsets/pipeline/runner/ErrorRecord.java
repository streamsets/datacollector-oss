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

import com.streamsets.pipeline.api.ErrorId;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.container.Utils;

import java.util.Locale;
import java.util.ResourceBundle;

public class ErrorRecord {

  public enum ERROR implements ErrorId {
    REQUIRED_FIELDS_MISSING("Required fields missing '{}'"),
    STAGE_CAUGHT_ERROR("Stage caught error: {}");

    private String template;

    ERROR(String template) {
      this.template = template;
    }

    @Override
    public String getMessageTemplate() {
      return template;
    }

    public String getMessage(String template, Object... args) {
      template = (template != null) ? template : this.template;
      return Utils.format(template, args);
    }
  }

  private final Record record;
  private final ERROR error;
  private final Object[] args;
  private Locale locale;

  public ErrorRecord(Record record, ERROR error, Object[] args) {
    this.record = record;
    this.error = error;
    this.args = args;
  }

  public void setLocale(Locale locale) {
    this.locale = locale;
  }

  public Record getRecord() {
    return record;
  }

  public ERROR getError() {
    return error;
  }

  public String getMessage() {
    // TODO localize
    return error.getMessage(null, args);
  }

}
