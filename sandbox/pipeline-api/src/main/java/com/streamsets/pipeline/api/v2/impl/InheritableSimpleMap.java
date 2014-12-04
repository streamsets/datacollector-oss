/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v2.impl;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.v2.SimpleMap;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class InheritableSimpleMap<K, V>implements SimpleMap<K,V> {
  private InheritableSimpleMap<K, V> parent;
  private Map<K, V> data;

  public InheritableSimpleMap() {
    data = new LinkedHashMap<K, V>();
  }

  public InheritableSimpleMap(SimpleMap<K, V> parent) {
    this();
    Preconditions.checkNotNull(parent, "parent cannot be NULL");
    this.parent = (InheritableSimpleMap<K, V>) parent;
  }

  @Override public boolean hasKey(K key) {
    Preconditions.checkNotNull(key, "key cannot be NULL");
    boolean contains;
    if (data.containsKey(key)) {
      contains = data.get(key) != null;
    } else {
      contains = (parent != null) && parent.hasKey(key);
    }
    return contains;
  }

  @Override public V get(K key) {
    Preconditions.checkNotNull(key, "key cannot be NULL");
    V value = null;
    if (data.containsKey(key)) {
      value = data.get(key);
    } else if (parent != null) {
      value = parent.get(key);
    }
    return value;
  }

  @Override public V put(K key, V value) {
    Preconditions.checkNotNull(key, "key cannot be NULL");
    Preconditions.checkNotNull(value, "value cannot be NULL");
    V oldValue = null;
    if (data.containsKey(key)) {
      oldValue = data.put(key, value);
    } else {
      if (parent != null) {
        oldValue = parent.get(key);
      }
      data.put(key, value);
    }
    return oldValue;
  }

  @Override public V remove(K key) {
    Preconditions.checkNotNull(key, "key cannot be NULL");
    V oldValue;
    if (data.containsKey(key)) {
      if (parent != null && parent.hasKey(key)) {
        oldValue = data.put(key, null);
      } else {
        oldValue = data.remove(key);
      }
    } else if (parent != null && parent.hasKey(key)) {
      oldValue = parent.get(key);
      data.put(key, null);
    } else {
      oldValue = null;
    }
    return oldValue;
  }

  @Override public Set<K> getKeys() {
    return Collections.unmodifiableSet(getKeySet(new LinkedHashSet<K>()));
  }

  private Set<K> getKeySet(Set<K> keys) {
    if (parent != null) {
      parent.getKeySet(keys);
    }
    for (Map.Entry<K, V> entry : data.entrySet()) {
      if (entry.getValue() == null) {
        keys.remove(entry.getKey());
      } else {
        keys.add(entry.getKey());
      }
    }
    return keys;
  }

}
