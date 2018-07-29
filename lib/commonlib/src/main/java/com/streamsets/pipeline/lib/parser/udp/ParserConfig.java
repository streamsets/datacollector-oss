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
package com.streamsets.pipeline.lib.parser.udp;

import java.util.HashMap;
import java.util.Map;

public class ParserConfig {
  Map<ParserConfigKey, Object> configs = new HashMap<>();

  public ParserConfig() {}

  public ParserConfig(ParserConfigKey key, Object value) {
    configs.put(key, value);
  }

  public ParserConfig(Map<ParserConfigKey, Object> other) {
    configs.putAll(other);
  }

  public String getString(ParserConfigKey key) {
    return (String) configs.get(key);
  }

  public long getLong(ParserConfigKey key) {
    return (long) configs.get(key);
  }

  public int getInteger(ParserConfigKey key) {
    return (int) configs.get(key);
  }

  public Boolean getBoolean(ParserConfigKey key) {
    return (Boolean)configs.get(key);
  }

  public Object get(ParserConfigKey key) {
    return configs.get(key);
  }

  public void putConfigs(Map<ParserConfigKey, String> other) {
    configs.putAll(other);
  }

  public void setConfigs(Map<ParserConfigKey, String> other) {
    configs.clear();
    putConfigs(other);
  }

  public void removeConfig(ParserConfigKey key) {
    configs.remove(key);
  }

  public void put(ParserConfigKey key, Object value) {
    configs.put(key, value);
  }

}
