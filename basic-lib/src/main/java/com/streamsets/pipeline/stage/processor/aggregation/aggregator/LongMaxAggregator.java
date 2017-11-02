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
 * Long Maximum Aggregator.
 */
public class LongMaxAggregator extends SimpleAggregator<LongMaxAggregator, Long> {

  public static class LongMaxAggregatable implements Aggregatable<LongMaxAggregator> {
    private String name;
    private long max;

    @Override
    public String getName() {
      return name;
    }

    public LongMaxAggregatable setName(String name) {
      this.name = name;
      return this;
    }

    public long getMax() {
      return max;
    }

    public LongMaxAggregatable setMax(long max) {
      this.max = max;
      return this;
    }
  }

  private class Data extends AggregatorData<LongMaxAggregator, Long> {
    private Long current;

    public Data(String name, long time) {
      super(name, time);
    }

    @Override
    public String getName() {
      return LongMaxAggregator.this.getName();
    }

    @Override
    public void process(Long value) {
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
    public synchronized Long get() {
      return current;
    }

    @Override
    public Aggregatable<LongMaxAggregator> getAggregatable() {
      return new LongMaxAggregatable().setName(getName()).setMax(get());
    }

    @Override
    public void aggregate(Aggregatable<LongMaxAggregator> aggregatable) {
      Utils.checkNotNull(aggregatable, "aggregatable");
      Utils.checkArgument(
          getName().equals(aggregatable.getName()),
          Utils.formatL("Aggregable '{}' does not match this aggregation '{}", aggregatable.getName(), getName())
      );
      Utils.checkArgument(aggregatable instanceof LongMaxAggregatable, Utils.formatL(
          "Aggregatable '{}' is a '{}' it should be '{}'",
          getName(),
          aggregatable.getClass().getSimpleName(),
          LongMaxAggregatable.class.getSimpleName()
      ));
      process(((LongMaxAggregatable) aggregatable).getMax());
    }
  }

  public LongMaxAggregator(String name) {
    super(Long.class, name);
  }

  public AggregatorData createAggregatorData(long timeWindowMillis) {
    return new Data(getName(), timeWindowMillis);
  }
}
