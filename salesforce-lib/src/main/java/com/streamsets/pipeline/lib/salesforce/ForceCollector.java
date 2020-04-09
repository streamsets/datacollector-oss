/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.lib.salesforce;

import com.streamsets.pipeline.api.Field;

import java.util.LinkedHashMap;

/**
 Collect records from ForceBulkReader so it can be used in different settings
 */
public abstract class ForceCollector {
  public void init() {
  }

  abstract public String addRecord(LinkedHashMap<String, Field> fields, int numRecords);

  abstract public String prepareQuery(String offset);

  abstract public boolean isDestroyed();

  public boolean isPreview() {
    return false;
  }

  public void queryCompleted() {
  }

  public boolean subscribeToStreaming() {
    return false;
  }

  public ForceRepeatQuery repeatQuery() {
    return ForceRepeatQuery.NO_REPEAT;
  }

  public String initialOffset() {
    return null;
  }
}
