/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
