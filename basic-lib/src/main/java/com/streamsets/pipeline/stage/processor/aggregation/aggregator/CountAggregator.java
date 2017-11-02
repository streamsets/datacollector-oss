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

import com.streamsets.pipeline.api.impl.Utils;

/**
 * Count Aggregator.
 */
public class CountAggregator extends SimpleAggregator<CountAggregator, Long> {

  public static class CounterAggregatable implements Aggregatable<CountAggregator> {
    private String name;
    private Long count;

    @Override
    public String getName() {
      return name;
    }

    public CounterAggregatable setName(String name) {
      this.name = name;
      return this;
    }

    public Long getCount() {
      return count;
    }

    public CounterAggregatable setCount(Long count) {
      this.count = count;
      return this;
    }
  }

  private class Data extends AggregatorData<CountAggregator, Long> {
    private long count;

    public Data(String name, long time) {
      super(name, time);
    }

    @Override
    public String getName() {
      return CountAggregator.this.getName();
    }

    @Override
    public void process(Long value) {
      if (value != null) {
        synchronized (this) {
          count += value;
        }
      }
    }

    @Override
    public synchronized Long get() {
      return count;
    }

    @Override
    public Aggregatable<CountAggregator> getAggregatable() {
      return new CounterAggregatable().setName(getName()).setCount(get());
    }

    @Override
    public void aggregate(Aggregatable<CountAggregator> aggregatable) {
      Utils.checkNotNull(aggregatable, "aggregatable");
      Utils.checkArgument(
          getName().equals(aggregatable.getName()),
          Utils.formatL("Aggregable '{}' does not match this aggregation '{}", aggregatable.getName(), getName())
      );
      process(((CounterAggregatable) aggregatable).getCount());
    }
  }

  public CountAggregator(String name) {
    super(Long.class, name);
  }

  @Override
  public AggregatorData createAggregatorData(long timeWindowMillis) {
    return new Data(getName(), timeWindowMillis);
  }

}
