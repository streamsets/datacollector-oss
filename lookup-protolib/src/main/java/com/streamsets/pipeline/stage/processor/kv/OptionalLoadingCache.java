/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.kv;

import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 * Wrapper for LoadingCache that enables implementation of cache miss and returning static
 * default values. The default values are always removed from the cache and hence values for
 * them will be looked up each time.
 */
public class OptionalLoadingCache<Key, Value> implements LoadingCache<Key, Optional<Value>> {

  private final boolean cacheMissingValues;
  private final LoadingCache<Key, Optional<Value>> delegate;
  private final Optional<Value> defaultValue;

  public OptionalLoadingCache(
    boolean cacheMissingValues,
    LoadingCache<Key, Optional<Value>> loadingCache,
    Optional<Value> defaultValue
  ) {
    this.cacheMissingValues = cacheMissingValues;
    this.delegate = loadingCache;
    this.defaultValue = defaultValue;
  }

  // Internal method to ensure that we return and not cache the default value if needed
  private Optional<Value> valueOrDefault(Key key, Optional<Value> value) {
    // If value is present simply return it
    if(value.isPresent()) {
      return value;
    }

    if(!cacheMissingValues) {
      delegate.invalidate(key);
    }

    return defaultValue;
  }

  @Override
  public Optional<Value> get(Key key) throws ExecutionException {
    return valueOrDefault(key, delegate.get(key));
  }

  @Override
  public Optional<Value> getUnchecked(Key key) {
    return valueOrDefault(key, delegate.getUnchecked(key));
  }

  @Override
  public ImmutableMap<Key, Optional<Value>> getAll(Iterable<? extends Key> keys) throws ExecutionException {
    throw new UnsupportedOperationException("getAll");
  }

  @Override
  public Optional<Value> apply(Key key) {
    return getUnchecked(key);
  }

  @Override
  public void refresh(Key key) {
    delegate.refresh(key);
  }

  @Override
  public ConcurrentMap<Key, Optional<Value>> asMap() {
    return delegate.asMap();
  }


  @Nullable
  @Override
  public Optional<Value> getIfPresent(Object key) {
    return delegate.getIfPresent(key);
  }

  @Override
  public Optional<Value> get(Key key, Callable<? extends Optional<Value>> valueLoader) throws ExecutionException {
    return valueOrDefault(key, delegate.get(key, valueLoader));
  }

  @Override
  public ImmutableMap<Key, Optional<Value>> getAllPresent(Iterable<?> keys) {
    return delegate.getAllPresent(keys);
  }

  @Override
  public void put(Key key, Optional<Value> value) {
    delegate.put(key, value);
  }

  @Override
  public void putAll(Map<? extends Key, ? extends Optional<Value>> m) {
    delegate.putAll(m);
  }

  @Override
  public void invalidate(Object key) {
    delegate.invalidate(key);
  }

  @Override
  public void invalidateAll(Iterable<?> keys) {
    delegate.invalidateAll(keys);
  }

  @Override
  public void invalidateAll() {
    delegate.invalidateAll();
  }

  @Override
  public long size() {
    return delegate.size();
  }

  @Override
  public CacheStats stats() {
    return delegate.stats();
  }

  @Override
  public void cleanUp() {
    delegate.cleanUp();
  }
}
