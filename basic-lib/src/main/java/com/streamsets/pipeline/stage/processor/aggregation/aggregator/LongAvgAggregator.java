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
 * Long Average Aggregator.
 */
public class LongAvgAggregator extends SimpleAggregator<LongAvgAggregator, Long> {

  public static class LongAvgAggregatable implements Aggregatable<LongAvgAggregator> {
    private String name;
    private long count;
    private long total;
    private long average;

    @Override
    public String getName() {
      return name;
    }

    public LongAvgAggregatable setName(String name) {
      this.name = name;
      return this;
    }

    public long getCount() {
      return count;
    }

    public LongAvgAggregatable setCount(long count) {
      this.count = count;
      return this;
    }

    public long getTotal() {
      return total;
    }

    public LongAvgAggregatable setTotal(long total) {
      this.total = total;
      return this;
    }

    public long getAverage() {
      return average;
    }

    public LongAvgAggregatable setAverage(long average) {
      this.average = average;
      return this;
    }
  }

  private class Data extends AggregatorData<LongAvgAggregator, Long> {
    private long count;
    private long total;

    public Data(String name, long time) {
      super(name, time);
    }

    @Override
    public String getName() {
      return LongAvgAggregator.this.getName();
    }

    @Override
    public synchronized void process(Long value) {
      if (value != null) {
        total += value;
        count++;
      }
    }

    @Override
    public synchronized Long get() {
      return (count == 0) ? null : (long) Math.rint((double)total / count);
    }

    @Override
    public synchronized Aggregatable<LongAvgAggregator> getAggregatable() {
      return new LongAvgAggregatable().setName(getName()).setCount(count).setTotal(total).setAverage(get());
    }

    @Override
    public void aggregate(Aggregatable<LongAvgAggregator> aggregatable) {
      Utils.checkNotNull(aggregatable, "aggregatable");
      Utils.checkArgument(
          getName().equals(aggregatable.getName()),
          Utils.formatL("Aggregable '{}' does not match this aggregation '{}", aggregatable.getName(), getName())
      );
      Utils.checkArgument(aggregatable instanceof LongAvgAggregatable, Utils.formatL(
          "Aggregatable '{}' is a '{}' it should be '{}'",
          getName(),
          aggregatable.getClass().getSimpleName(),
          LongAvgAggregatable.class.getSimpleName()
      ));
      synchronized (this) {
        count += ((LongAvgAggregatable) aggregatable).getCount();
        total += ((LongAvgAggregatable) aggregatable).getTotal();
      }
    }
  }

  public LongAvgAggregator(String name) {
    super(Long.class, name);
  }

  @Override
  public AggregatorData createAggregatorData(long timeWindowMillis) {
    return new LongAvgAggregator.Data(getName(), timeWindowMillis);
  }

}
