/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.record;

import java.util.Set;

public interface SimpleMap<K, V> {

  public Set<K> getKeys();

  public boolean hasKey(K key);

  public V get(K key);

  public V put(K key, V value);

  public V remove(K key);

}
