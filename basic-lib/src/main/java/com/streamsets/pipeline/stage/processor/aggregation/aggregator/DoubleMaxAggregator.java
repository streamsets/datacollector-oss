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
 * Double Maximum Aggregator.
 */
public class DoubleMaxAggregator extends SimpleAggregator<DoubleMaxAggregator, Double> {

  public static class DoubleMaxAggregatable implements Aggregatable<DoubleMaxAggregator> {
    private String name;
    private double max;

    @Override
    public String getName() {
      return name;
    }

    public DoubleMaxAggregatable setName(String name) {
      this.name = name;
      return this;
    }

    public double getMax() {
      return max;
    }

    public DoubleMaxAggregatable setMax(double max) {
      this.max = max;
      return this;
    }
  }

  private class Data extends AggregatorData<DoubleMaxAggregator, Double> {
    private Double current;

    public Data(String name, long time) {
      super(name, time);
    }

    @Override
    public String getName() {
      return DoubleMaxAggregator.this.getName();
    }

    @Override
    public void process(Double value) {
      if (value != null) {
        synchronized (this) {
          if (current == null) {
            current = value;
          } else if (value > current) {
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
    public Aggregatable<DoubleMaxAggregator> getAggregatable() {
      return new DoubleMaxAggregatable().setName(getName()).setMax(get());
    }

    @Override
    public void aggregate(Aggregatable<DoubleMaxAggregator> aggregatable) {
      Utils.checkNotNull(aggregatable, "aggregatable");
      Utils.checkArgument(
          getName().equals(aggregatable.getName()),
          Utils.formatL("Aggregable '{}' does not match this aggregation '{}", aggregatable.getName(), getName())
      );
      Utils.checkArgument(aggregatable instanceof DoubleMaxAggregatable, Utils.formatL(
          "Aggregatable '{}' is a '{}' it should be '{}'",
          getName(),
          aggregatable.getClass().getSimpleName(),
          DoubleMaxAggregatable.class.getSimpleName()
      ));
      process(((DoubleMaxAggregatable) aggregatable).getMax());
    }
  }

  public DoubleMaxAggregator(String name) {
    super(Double.class, name);
  }

  public AggregatorData createAggregatorData(long timeWindowMillis) {
    return new Data(getName(), timeWindowMillis);
  }

}
