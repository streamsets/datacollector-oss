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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ReservedPrefixSimpleMap<V> implements SimpleMap<String, V> {
  private String reservedPrefix;
  private SimpleMap<String, V> map;

  public ReservedPrefixSimpleMap(String reservedPrefix, SimpleMap<String, V> map) {
    Preconditions.checkNotNull(reservedPrefix, "reservedPrefix cannot be null");
    Preconditions.checkArgument(!reservedPrefix.isEmpty(), "reservedPrefix cannot be empty");
    this.reservedPrefix = reservedPrefix;
    this.map = Preconditions.checkNotNull(map, "map cannot be null");
  }

  private boolean isReserved(String key) {
    Preconditions.checkNotNull(key, "key cannot be null");
    return key.startsWith(reservedPrefix);
  }

  private void assertKey(String key) {
    Preconditions.checkArgument(!isReserved(key), String.format("Invalid key, cannot start with '%s'", reservedPrefix));
  }

  @Override
  public Set<String> getKeys() {
    Set<String> keys = new HashSet<String>();
    for (String key : map.getKeys()) {
      if (!key.startsWith(reservedPrefix)) {
        keys.add(key);
      }
    }
    return Collections.unmodifiableSet(keys);
  }

  @Override
  public boolean hasKey(String key) {
    assertKey(key);
    return map.hasKey(key);
  }

  @Override
  public V get(String key) {
    assertKey(key);
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

  @Override
  public Map<String, V> getValues() {
    Map<String, V> map = new HashMap<String, V>();
    for (String key : getKeys()) {
      map.put(key, get(key));
    }
    return map;
  }

}
