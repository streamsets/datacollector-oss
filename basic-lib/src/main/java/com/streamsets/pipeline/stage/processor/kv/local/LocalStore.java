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
package com.streamsets.pipeline.stage.processor.kv.local;

import com.google.common.base.Optional;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class LocalStore {

  private final LocalLookupConfig conf;

  public LocalStore(LocalLookupConfig conf) {
    this.conf = conf;
  }

  public Optional<String> get(String key) {
    return Optional.fromNullable(conf.values.get(key));
  }

  public Map<String, Optional<String>> get(Collection<String> keys) {
    Map<String, Optional<String>> values = new HashMap<>();
    for (String key : keys) {
      values.put(key, Optional.fromNullable(conf.values.get(key)));
    }
    return values;
  }

  public void put(String key, String value) {
    conf.values.put(key, value);
  }

  public void putAll(Map<String, String> entries) {
    conf.values.putAll(entries);
  }

  public void close() throws Exception {
    // no-op
  }
}
