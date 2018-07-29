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
 * Double Average Aggregator.
 */
public class DoubleSumAggregator extends SimpleAggregator<DoubleSumAggregator, Double> {

  public static class DoubleSumAggregatable implements Aggregatable<DoubleSumAggregator> {
    private String name;
    private double sum;
    private long count;

    @Override
    public String getName() {
      return name;
    }

    public DoubleSumAggregatable setName(String name) {
      this.name = name;
      return this;
    }

    public double getSum() {
      return sum;
    }

    public double getCount() {
      return count;
    }

    public DoubleSumAggregatable setSum(double sum) {
      this.sum = sum;
      return this;
    }

    public DoubleSumAggregatable setCount(long count) {
      this.count = count;
      return this;
    }
  }

  private class Data extends AggregatorData<DoubleSumAggregator, Double> {
    private double sum;
    private long count;

    public Data(String name, long time) {
      super(name, time);
    }

    @Override
    public String getName() {
      return DoubleSumAggregator.this.getName();
    }

    @Override
    public void process(Double value) {
      if (value != null) {
        synchronized (this) {
          sum += value;
          count++;
        }
      }
    }

    @Override
    public synchronized Double get() {
      return (count == 0) ? null : sum;
    }

    @Override
    public Aggregatable<DoubleSumAggregator> getAggregatable() {
      DoubleSumAggregatable aggregatable = new DoubleSumAggregatable().setName(getName());
      synchronized (this) {
        aggregatable.setCount(count).setSum(sum);
      }
      return aggregatable;
    }

    @Override
    public void aggregate(Aggregatable<DoubleSumAggregator> aggregatable) {
      Utils.checkNotNull(aggregatable, "aggregatable");
      Utils.checkArgument(
          getName().equals(aggregatable.getName()),
          Utils.formatL("Aggregable '{}' does not match this aggregation '{}", aggregatable.getName(), getName())
      );
      Utils.checkArgument(aggregatable instanceof DoubleSumAggregatable, Utils.formatL(
          "Aggregatable '{}' is a '{}' it should be '{}'",
          getName(),
          aggregatable.getClass().getSimpleName(),
          DoubleSumAggregatable.class.getSimpleName()
      ));
      synchronized (this) {
        sum += ((DoubleSumAggregatable) aggregatable).getSum();
        count += ((DoubleSumAggregatable) aggregatable).getCount();
      }
    }
  }

  public DoubleSumAggregator(String name) {
    super(Double.class, name);
  }

  @Override
  public AggregatorData createAggregatorData(long timeWindowMillis) {
    return new Data(getName(), timeWindowMillis);
  }

}
