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
 * Double Standard Deviation Aggregator.
 */
public class DoubleStdDevAggregator extends SimpleAggregator<DoubleStdDevAggregator, Number> {

  public static class DoubleStdDevAggregatable implements Aggregatable<DoubleStdDevAggregator> {
    private String name;
    private long count;
    private double total;
    private double totalSquare;
    private double stdDev;

    @Override
    public String getName() {
      return name;
    }

    public DoubleStdDevAggregatable setName(String name) {
      this.name = name;
      return this;
    }

    public long getCount() {
      return count;
    }

    public DoubleStdDevAggregatable setCount(long count) {
      this.count = count;
      return this;
    }

    public double getTotal() {
      return total;
    }

    public DoubleStdDevAggregatable setTotal(double total) {
      this.total = total;
      return this;
    }

    public double getTotalSquare() {
      return totalSquare;
    }

    public DoubleStdDevAggregatable setTotalSquare(double totalSquare) {
      this.totalSquare = totalSquare;
      return this;
    }

    public double getStdDev() {
      return stdDev;
    }

    public DoubleStdDevAggregatable setStdDev(double stdDev) {
      this.stdDev = stdDev;
      return this;
    }
  }

  private class Data extends AggregatorData<DoubleStdDevAggregator, Double> {
    private long count;
    private double total;
    private double totalSquare;

    public Data(String name, long time) {
      super(name, time);
    }

    @Override
    public String getName() {
      return DoubleStdDevAggregator.this.getName();
    }

    @Override
    public void process(Double value) {
      if (value != null) {
        synchronized (this) {
          count++;
          total += value;
          totalSquare += value * value;
        }
      }
    }

    @Override
    public synchronized Double get() {
      return (count < 2) ? -1 : Math.sqrt((count * totalSquare - total * total) / (count * (count - 1)));
    }

    @Override
    public Aggregatable<DoubleStdDevAggregator> getAggregatable() {
      DoubleStdDevAggregatable aggregatable = new DoubleStdDevAggregatable().setName(getName());
      synchronized (this) {
        aggregatable.setCount(count).setTotal(total).setTotalSquare(totalSquare).setStdDev(get());
      }
      return aggregatable;
    }

    @Override
    public void aggregate(Aggregatable<DoubleStdDevAggregator> aggregatable) {
      Utils.checkNotNull(aggregatable, "aggregatable");
      Utils.checkArgument(
          getName().equals(aggregatable.getName()),
          Utils.formatL("Aggregable '{}' does not match this aggregation '{}", aggregatable.getName(), getName())
      );
      Utils.checkArgument(aggregatable instanceof DoubleStdDevAggregatable, Utils.formatL(
          "Aggregatable '{}' is a '{}' it should be '{}'",
          getName(),
          aggregatable.getClass().getSimpleName(),
          DoubleStdDevAggregatable.class.getSimpleName()
      ));
      synchronized (this) {
        count += ((DoubleStdDevAggregatable) aggregatable).getCount();
        total += ((DoubleStdDevAggregatable) aggregatable).getTotal();
        totalSquare += ((DoubleStdDevAggregatable) aggregatable).getTotalSquare();
      }
    }
  }

  public DoubleStdDevAggregator(String name) {
    super(Double.class, name);
  }

  public AggregatorData createAggregatorData(long timeWindowMillis) {
    return new Data(getName(), timeWindowMillis);
  }

}
