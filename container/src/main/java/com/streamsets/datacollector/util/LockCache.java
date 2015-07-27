/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */

package com.streamsets.datacollector.util;

import javax.inject.Inject;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Cache providing mapping of keys to values. Entries stay in cache till
 * they are evicted.
 * @param <K>
 */
public class LockCache<K> {
  private LoadingCache<K, Object> cache;

  @Inject
  public LockCache() {
    cache = CacheBuilder.newBuilder().weakValues().build(new CacheLoader<K, Object>() {
      @Override
      public Object load(K key) {
        return new Object();
      }
    });
  }

  /**
   * Returns value associated with resource in cache if exists, first loading that value if necessary.
   * @param resource the key in cache
   * @return
   */
  public Object getLock(K resource) {
    return cache.getUnchecked(resource);
  }

}


