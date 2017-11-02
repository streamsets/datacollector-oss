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

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Group-by Aggregator supporting all Simple Aggregators as group-by element.
 */
public class GroupByAggregator<A extends SimpleAggregator, T> extends Aggregator<GroupByAggregator, Map<String, T>> {

  public static class GroupByAggregatable implements Aggregatable<GroupByAggregator> {
    private String name;
    private Map<String, Aggregatable> groups;

    @Override
    public String getName() {
      return name;
    }

    public GroupByAggregatable setName(String name) {
      this.name = name;
      return this;
    }

    public Map<String, Aggregatable> getGroups() {
      return groups;
    }

    public GroupByAggregatable setGroups(Map<String, Aggregatable> groups) {
      this.groups = groups;
      return this;
    }
  }

  class Data extends AggregatorData<GroupByAggregator<A, T>, Map<String, T>> {
    private final Map<String, AggregatorData<SimpleAggregator, Number>> groups;
    private final ReentrantReadWriteLock rwLock;

    public Data(String name, long time) {
      super(name, time);
      groups = new HashMap<>();
      rwLock = new ReentrantReadWriteLock(true);
    }

    @Override
    public String getName() {
      return GroupByAggregator.this.getName();
    }

    @Override
    public void process(Map<String, T> value) {
      for (Map.Entry<String, T> entry : value.entrySet()) {
        process(entry.getKey(), entry.getValue());
      }
    }

    @SuppressWarnings("unchecked")
    protected void process(String group, T value) {
      AggregatorData aggregatorData;
      rwLock.writeLock().lock();
      try {
        aggregatorData = groups.computeIfAbsent(group,
            k -> GroupByAggregator.this.createElementAggregatorData(group, getTime())
        );
      } finally {
        rwLock.writeLock().unlock();
      }
      aggregatorData.process(value);
    }


    @Override
    @SuppressWarnings("unchecked")
    public Map<String, T> get() {
      rwLock.readLock().lock();
      try {
        Map<String, T> map = new HashMap<>();
        for (Map.Entry<String, AggregatorData<SimpleAggregator, Number>> group : groups.entrySet()) {
          map.put(group.getKey(), (T) group.getValue().get());
        }
        return map;
      } finally {
        rwLock.readLock().unlock();
      }
    }

    @SuppressWarnings("unchecked")
    public AggregatorData<SimpleAggregator, Number> getGroupByElementData(String groupName) {
      rwLock.readLock().lock();
      try {
        return groups.get(groupName);
      } finally {
        rwLock.readLock().unlock();
      }
    }

    public Set<String> getGroupByElements() {
      rwLock.readLock().lock();
      try {
        return new HashSet<>(groups.keySet());
      } finally {
        rwLock.readLock().unlock();
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Aggregatable<GroupByAggregator<A, T>> getAggregatable() {
      GroupByAggregatable aggregatable = new GroupByAggregatable().setName(getName());
      rwLock.readLock().lock();
      try {
        Map<String, Aggregatable> aggregatableGroups = new HashMap<>();
        for (Map.Entry<String, AggregatorData<SimpleAggregator, Number>> group : groups.entrySet()) {
          aggregatableGroups.put(group.getKey(), group.getValue().getAggregatable());
        }
        aggregatable.setGroups(aggregatableGroups);
      } finally {
        rwLock.readLock().unlock();
      }
      return (Aggregatable) aggregatable;
    }

    @Override
    public void aggregate(Aggregatable aggregatable) {
      Utils.checkNotNull(aggregatable, "aggregatable");
      Utils.checkArgument(getName().equals(aggregatable.getName()),
          Utils.formatL("Aggregable '{}' does not match this aggregation '{}", aggregatable.getName(), getName())
      );
      Utils.checkArgument(aggregatable instanceof GroupByAggregatable, Utils.formatL(
          "Aggregatable '{}' is a '{}' it should be '{}'",
          getName(),
          aggregatable.getClass().getSimpleName(),
          GroupByAggregatable.class.getSimpleName()
      ));

      rwLock.writeLock().lock();
      try {
        for (Map.Entry<String, Aggregatable> entry : ((GroupByAggregatable) aggregatable).getGroups().entrySet()) {
          AggregatorData aggregatorData = groups.computeIfAbsent(entry.getKey(),
              groupByElementName -> GroupByAggregator.this.createElementAggregatorData(groupByElementName, getTime())
          );
          aggregatorData.aggregate(entry.getValue());
        }
      } finally {
        rwLock.writeLock().unlock();
      }
    }
  }

  private final Class<A> aggregatorKlass;
  private final Aggregators aggregators;

  /**
   * Group-by Aggregator constructor.
   *  @param name name of the aggregator.
   * @param aggregatorKlass Aggregator type to use with each group-by element.
   * @param aggregators Aggregators that is creating the group-by aggregator.
   */
  @SuppressWarnings("unchecked")
  GroupByAggregator(
      String name, Class<A> aggregatorKlass, Aggregators aggregators
  ) {
    super(aggregators.getAggregatorUnit(aggregatorKlass), name);
    this.aggregators = aggregators;
    this.aggregatorKlass = aggregatorKlass;
  }

  /**
   * Returns the Aggregator type to use with each group-by element.
   *
   * @return the Aggregator type to use with each group-by element.
   */
  protected Class<A> getAggregatorClass() {
    return aggregatorKlass;
  }

  /**
   * Returns the Aggregators that created the group-by aggregator.
   * @return the Aggregators that created the group-by aggregator.
   */
  protected Aggregators getAggregators() {
    return aggregators;
  }

  /**
   * Returns the AggregatorData of the group-by aggregator.
   *
   * @return the AggregatorData of the group-by aggregator.
   * @param timeWindowMillis
   */
  @Override
  public AggregatorData createAggregatorData(long timeWindowMillis) {
    return new Data(getName(), timeWindowMillis);
  }

  public AggregatorData createElementAggregatorData(String elementName, long timeWindowMillis) {
    return aggregators.createAggregatorData(getAggregatorClass(), elementName, timeWindowMillis);
  }

  /**
   * Processes the given value into the corresponding group-by element of the aggregator.
   *
   * @param group the group-by element.
   * @param value the value to process.
   */
  public void process(String group, T value) {
    getData().process(ImmutableMap.of(group, value));
  }

}
