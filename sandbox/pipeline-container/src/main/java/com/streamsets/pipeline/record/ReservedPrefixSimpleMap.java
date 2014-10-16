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
package com.streamsets.pipeline.record;

import com.google.common.base.Preconditions;

import java.util.Set;

public class ReservedPrefixSimpleMap<V> implements SimpleMap<String, V> {
  private String reservedPrefix;
  private SimpleMap<String, V> map;

  public ReservedPrefixSimpleMap(String reservedPrefix, SimpleMap<String, V> map) {
    this.reservedPrefix = reservedPrefix;
    this.map = map;
  }

  private void assertKey(String key) {
    Preconditions.checkNotNull(key, "key cannot be null");
    Preconditions.checkArgument(key.startsWith(reservedPrefix), String.format(
        "Invalid key, cannot start with '%s'", reservedPrefix));
  }
  @Override
  public Set<String> getKeys() {
    return map.getKeys();
  }

  @Override
  public boolean hasKey(String key) {
    return map.hasKey(key);
  }

  @Override
  public V get(String key) {
    return map.get(key);
  }

  @Override
  public V put(String key, V value) {
    assertKey(key);
    return map.put(key, value);
  }

  @Override
  public V remove(String key) {
    assertKey(key);
    return map.remove(key);
  }

}
