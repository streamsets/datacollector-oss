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
package com.streamsets.datacollector.metrics;

import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

public class MetricsCache<K, V> implements Cache<K, V> {

  public final Cache<K, V> delegate;

  static String KEY_COUNT = "count";
  private final Map<String, Object> gaugeMap;

  public MetricsCache(MetricRegistry metrics, String name, Cache delegate) {
    this.delegate = delegate;

    this.gaugeMap = MetricsConfigurator.createFrameworkGauge(
      metrics,
      "metrics-cache." + name,
      "runtime",
      null
    ).getValue();
    this.gaugeMap.put(KEY_COUNT, 0);
  }

  @Nullable
  @Override
  public V getIfPresent(Object key) {
    try {
      return delegate.getIfPresent(key);
    } finally {
      updateGaugeMap();
    }
  }

  @Override
  public V get(K key, Callable<? extends V> valueLoader) throws ExecutionException {
    try {
      return delegate.get(key, valueLoader);
    } finally {
      updateGaugeMap();
    }
  }

  @Override
  public ImmutableMap<K, V> getAllPresent(Iterable<?> keys) {
    try {
      return delegate.getAllPresent(keys);
    } finally {
      updateGaugeMap();
    }
  }

  @Override
  public void put(K key, V value) {
    delegate.put(key, value);
    updateGaugeMap();
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    delegate.putAll(m);
    updateGaugeMap();
  }

  @Override
  public void invalidate(Object key) {
    delegate.invalidate(key);
    updateGaugeMap();
  }

  @Override
  public void invalidateAll(Iterable<?> keys) {
    delegate.invalidateAll(keys);
    updateGaugeMap();
  }

  @Override
  public void invalidateAll() {
    delegate.invalidateAll();
    updateGaugeMap();
  }

  @Override
  public long size() {
    try {
      return delegate.size();
    } finally {
      updateGaugeMap();
    }
  }

  @Override
  public CacheStats stats() {
    try {
      return delegate.stats();
    } finally {
      updateGaugeMap();
    }
  }

  @Override
  public ConcurrentMap<K, V> asMap() {
    try {
      return delegate.asMap();
    } finally {
      updateGaugeMap();
    }
  }

  @Override
  public void cleanUp() {
    delegate.cleanUp();
    updateGaugeMap();
  }

  private void updateGaugeMap() {
    this.gaugeMap.put(KEY_COUNT, delegate.size());
  }
}
