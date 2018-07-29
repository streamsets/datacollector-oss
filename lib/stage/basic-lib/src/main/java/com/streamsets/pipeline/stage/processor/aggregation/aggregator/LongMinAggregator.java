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
 * Long Minimum Aggregator.
 */
public class LongMinAggregator extends SimpleAggregator<LongMinAggregator, Long> {

  public static class LongMinAggregatable implements Aggregatable<LongMinAggregator> {
    private String name;
    private long min;

    @Override
    public String getName() {
      return name;
    }

    public LongMinAggregatable setName(String name) {
      this.name = name;
      return this;
    }

    public long getMin() {
      return min;
    }

    public LongMinAggregatable setMin(long min) {
      this.min = min;
      return this;
    }
  }

  private class Data extends AggregatorData<LongMinAggregator, Long> {
    private Long current;

    public Data(String name, long time) {
      super(name, time);
    }

    @Override
    public String getName() {
      return LongMinAggregator.this.getName();
    }

    @Override
    public void process(Long value) {
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
    public synchronized Long get() {
      return current;
    }

    @Override
    public Aggregatable<LongMinAggregator> getAggregatable() {
      return new LongMinAggregatable().setName(getName()).setMin(get());
    }

    @Override
    public void aggregate(Aggregatable<LongMinAggregator> aggregatable) {
      Utils.checkNotNull(aggregatable, "aggregatable");
      Utils.checkArgument(
          getName().equals(aggregatable.getName()),
          Utils.formatL("Aggregable '{}' does not match this aggregation '{}", aggregatable.getName(), getName())
      );
      Utils.checkArgument(aggregatable instanceof LongMinAggregatable, Utils.formatL(
          "Aggregatable '{}' is a '{}' it should be '{}'",
          getName(),
          aggregatable.getClass().getSimpleName(),
          LongMinAggregatable.class.getSimpleName()
      ));
      process(((LongMinAggregatable) aggregatable).getMin());
    }
  }

  public LongMinAggregator(String name) {
    super(Long.class, name);
  }

  public AggregatorData createAggregatorData(long timeWindowMillis) {
    return new Data(getName(), timeWindowMillis);
  }

}
