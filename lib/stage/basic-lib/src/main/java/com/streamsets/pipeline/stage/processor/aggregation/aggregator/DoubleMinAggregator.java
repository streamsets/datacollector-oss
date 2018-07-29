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
 * Double Minimum Aggregator.
 */
public class DoubleMinAggregator extends SimpleAggregator<DoubleMinAggregator, Double> {

  public static class DoubleMinAggregatable implements Aggregatable<DoubleMinAggregator> {
    private String name;
    private double min;

    @Override
    public String getName() {
      return name;
    }

    public DoubleMinAggregatable setName(String name) {
      this.name = name;
      return this;
    }

    public double getMin() {
      return min;
    }

    public DoubleMinAggregatable setMin(double min) {
      this.min = min;
      return this;
    }
  }

  private class Data extends AggregatorData<DoubleMinAggregator, Double> {
    private Double current;

    public Data(String name, long time) {
      super(name, time);
    }

    @Override
    public String getName() {
      return DoubleMinAggregator.this.getName();
    }

    @Override
    public void process(Double value) {
      if (value != null) {
        synchronized (this) {
          if (current == null) {
            current = value;
          } else if (value < current) {
            current = value;
          }
        }
      }
    }

    @Override
    public synchronized Double get() {
      return current;
    }

    @Override
    public Aggregatable<DoubleMinAggregator> getAggregatable() {
      return new DoubleMinAggregatable().setName(getName()).setMin(get());
    }

    @Override
    public void aggregate(Aggregatable<DoubleMinAggregator> aggregatable) {
      Utils.checkNotNull(aggregatable, "aggregatable");
      Utils.checkArgument(
          getName().equals(aggregatable.getName()),
          Utils.formatL("Aggregable '{}' does not match this aggregation '{}", aggregatable.getName(), getName())
      );
      Utils.checkArgument(aggregatable instanceof DoubleMinAggregatable, Utils.formatL(
          "Aggregatable '{}' is a '{}' it should be '{}'",
          getName(),
          aggregatable.getClass().getSimpleName(),
          DoubleMinAggregatable.class.getSimpleName()
      ));
      process(((DoubleMinAggregatable) aggregatable).getMin());
    }
  }

  public DoubleMinAggregator(String name) {
    super(Double.class, name);
  }

  public AggregatorData createAggregatorData(long timeWindowMillis) {
    return new Data(getName(), timeWindowMillis);
  }

}
