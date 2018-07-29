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
package com.streamsets.pipeline.stage.processor.aggregation.aggregator;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.streamsets.pipeline.stage.processor.aggregation.WindowType;

import java.util.AbstractMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Minimal Map implementation to expose real time simple aggregator data through a gauge.
 */
public class SimpleValueGaugeMap extends AbstractMap {
  private final WindowType windowType;
  private final SimpleAggregator aggregator;

  private static class GaugeMapEntry implements Entry {
    private final String key;
    private final Object value;

    public GaugeMapEntry(String key, Object value) {
      this.key = key;
      this.value = value;
    }

    public GaugeMapEntry(AggregatorDataProvider.DataWindow dataWindow, Aggregator aggregator) {
      this(Long.toString(dataWindow.getEndTimeMillis()), dataWindow.getData(aggregator).get());
    }

    @Override
    public Object getKey() {
      return key;
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public Object setValue(Object value) {
      throw new UnsupportedOperationException();
    }
  }

  public SimpleValueGaugeMap(WindowType windowType, SimpleAggregator aggregator) {
    this.windowType = windowType;
    this.aggregator = aggregator;
  }

  public boolean isEmpty() {
    // returns false to trigger the scanning when JSON serializing
    return false;
  }

  @Override
  public Set<Entry> entrySet() {
    List<AggregatorDataProvider.DataWindow> list = aggregator.getDataProvider().getDataWindows();
    Set<Entry> entrySet;
    if (windowType == WindowType.ROLLING) {
      entrySet = new LinkedHashSet<>(Lists.transform(list, input -> new GaugeMapEntry(input, aggregator)));
    } else {
      // we need to aggregate all microwindows to produce one

      //time of current open timewindow
      long endTimeMillis= list.get(list.size() - 1).getEndTimeMillis();

      AggregatorData aggregatorData = aggregator.createAggregatorData(endTimeMillis);
      for (AggregatorDataProvider.DataWindow dataWindow : list) {
        aggregatorData.aggregate(dataWindow.getData(aggregator).getAggregatable());
      }
      String key = Long.toString(endTimeMillis);
      Object value = aggregatorData.get();
      entrySet = ImmutableSet.of(new GaugeMapEntry(key, value));
    }
    return entrySet;
  }

  @Override
  public Object getOrDefault(Object key, Object defaultValue) {
    return null;
  }

  @Override
  public void forEach(BiConsumer action) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void replaceAll(BiFunction function) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object putIfAbsent(Object key, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object key, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean replace(Object key, Object oldValue, Object newValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object replace(Object key, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object computeIfAbsent(Object key, Function mappingFunction) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object computeIfPresent(Object key, BiFunction remappingFunction) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object compute(Object key, BiFunction remappingFunction) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object merge(Object key, Object value, BiFunction remappingFunction) {
    throw new UnsupportedOperationException();
  }

}
