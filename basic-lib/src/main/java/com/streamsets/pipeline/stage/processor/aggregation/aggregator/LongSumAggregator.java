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
public class LongSumAggregator extends SimpleAggregator<LongSumAggregator, Long> {

  public static class LongSumAggregatable implements Aggregatable<LongSumAggregator> {
    private String name;
    private long sum;
    private long count;

    @Override
    public String getName() {
      return name;
    }

    public LongSumAggregatable setName(String name) {
      this.name = name;
      return this;
    }

    public long getSum() {
      return sum;
    }

    public double getCount() {
      return count;
    }

    public LongSumAggregatable setSum(long sum) {
      this.sum = sum;
      return this;
    }

    public LongSumAggregatable setCount(long count) {
      this.count = count;
      return this;
    }
  }

  private class Data extends AggregatorData<LongSumAggregator, Long> {
    private long sum;
    private long count;

    public Data(String name, long time) {
      super(name, time);
    }

    @Override
    public String getName() {
      return LongSumAggregator.this.getName();
    }

    @Override
    public void process(Long value) {
      if (value != null) {
        synchronized (this) {
          sum += value;
          count++;
        }
      }
    }

    @Override
    public synchronized Long get() {
      return (count == 0) ? null : sum;
    }

    @Override
    public Aggregatable<LongSumAggregator> getAggregatable() {
      LongSumAggregatable aggregatable = new LongSumAggregatable().setName(getName());
      synchronized (this) {
        aggregatable.setCount(count).setSum(sum);
      }
      return aggregatable;
    }

    @Override
    public void aggregate(Aggregatable<LongSumAggregator> aggregatable) {
      Utils.checkNotNull(aggregatable, "aggregatable");
      Utils.checkArgument(
          getName().equals(aggregatable.getName()),
          Utils.formatL("Aggregable '{}' does not match this aggregation '{}", aggregatable.getName(), getName())
      );
      Utils.checkArgument(aggregatable instanceof LongSumAggregatable, Utils.formatL(
          "Aggregatable '{}' is a '{}' it should be '{}'",
          getName(),
          aggregatable.getClass().getSimpleName(),
          LongSumAggregatable.class.getSimpleName()
      ));
      synchronized (this) {
        sum += ((LongSumAggregatable) aggregatable).getSum();
        count += ((LongSumAggregatable) aggregatable).getCount();
      }
    }
  }

  public LongSumAggregator(String name) {
    super(Long.class, name);
  }

  @Override
  public AggregatorData createAggregatorData(long timeWindowMillis) {
    return new Data(getName(), timeWindowMillis);
  }

}
