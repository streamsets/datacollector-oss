/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.processor.kv.local;

import com.google.common.base.Optional;
import com.streamsets.pipeline.stage.processor.kv.Store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class LocalStore implements Store {

  private final LocalLookupConfig conf;

  public LocalStore(LocalLookupConfig conf) {
    this.conf = conf;
  }

  @Override
  public Optional<String> get(String key) {
    return Optional.fromNullable(conf.values.get(key));
  }

  @Override
  public List<Optional<String>> get(Collection<String> keys) {
    List<Optional<String>> values = new ArrayList<>(keys.size());
    for (String key : keys) {
      values.add(Optional.fromNullable(conf.values.get(key)));
    }
    return values;
  }

  @Override
  public void put(String key, String value) {
    conf.values.put(key, value);
  }

  @Override
  public void putAll(Map<String, String> entries) {
    conf.values.putAll(entries);
  }

  @Override
  public void close() throws Exception {
    // no-op
  }
}
