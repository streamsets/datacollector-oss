/*
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.EvictingQueue;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The AggregatorDataProvider is responsible for creating and providing AggregatorData structures to a set of
 * Aggregator instances.
 * <p/>
 * The AggregatorDataProvider also stores a configured number of data endTimeMillis windows of
 * aggregated data for all its registered Aggregators.
 * <p/>
 * The creation of the AggregatorData structures itself is done by requesting the creation of the same to the
 * corresponding Aggregator instance.
 * <p/>
 * By providing the AggregatorData to a set of Aggregators, the AggregatorDataProvider has the capability of
 * atomically replacing the AggregatorData for all registered Aggregators with no contention.
 */
public class AggregatorDataProvider {

  /**
   * A DataWindow contains the aggregated data for all the Aggregators registered with the AggregatorDataProvider for a
   * particular endTimeMillis window.
   * <p/>
   * If the endTimeMillis window has passed, all the aggregated data is immutable. If the endTimeMillis window is the
   * current endTimeMillis window the aggregated data will be updated in place as Aggregators process data.
   * <p/>
   * When a DataWindow is created, it is always created for the current time window and it will reflect live aggregated
   * data. When the time window closes, by invoking the {@link #setDataAndClose(Map)} method, the final aggregated data
   * of the time window is provided and that will be returned from that point onwards until the DataWindow is
   * discarded.
   * <p/>
   * For GroupByAggregator instance, the DataWindow provides a reverse lookup to retrieve the data for all the groupby
   * elements of the GroupByAggregator.
   */
  class DataWindow {
    private final long endTimeMillis;
    private volatile Map<Aggregator, AggregatorData> data;
    private volatile Map<GroupByAggregator, Map<String, Aggregator>> groupByElementAggregators;

    /**
     * Creates a DataWindow.
     *
     * @param endTimeMillis end time of the DataWindow, Unix epoch time in millis.
     */
    private DataWindow(long endTimeMillis) {
      this.endTimeMillis = endTimeMillis;
      this.groupByElementAggregators = new ConcurrentHashMap<>();
    }

    /**
     * Returns a Map with all the group-by element Aggregators of all GroupByAggregators.
     *
     * @return a Map with all the group-by element Aggregators of all GroupByAggregators.
     */
    Map<GroupByAggregator, Map<String, Aggregator>> getAllGroupByElementAggregators() {
      return groupByElementAggregators;
    }

    /**
     * Returns the DataWindow end time.
     *
     * @return the DataWindow end time, Unix epoch time in millis.
     */
    long getEndTimeMillis() {
      return endTimeMillis;
    }

    /**
     * Returns all the group-by element Aggregators of a GroupByAggregator.
     *
     * @param parentGroupByAggregator the group-by Aggregator.
     * @return all the group-by element Aggregators of the given GroupByAggregator.
     */
    Map<String, Aggregator> getGroupByElementAggregators(GroupByAggregator parentGroupByAggregator) {
      Map<String, Aggregator> map = groupByElementAggregators.get(parentGroupByAggregator);
      // only if it is the live datawindow (data == null) we clone group-by elements to have a fixed set of elements.
      // For closed datawindows it cannot change, so no need to clone it.
      if (data == null) {
        map = (map == null) ? new HashMap<>() : new HashMap<>(map);
      }
      return map;
    }

    /**
     * Returns the AggregatorData for an Aggregator.
     *
     * @param aggregator the Aggregator to retrieve the AggregatorData.
     * @return the AggregatorData, <b>NULL</b> if none.
     */
    AggregatorData getData(Aggregator aggregator) {
      return (data != null) ? data.get(aggregator) : AggregatorDataProvider.this.get().get(aggregator);
    }

    /**
     * Sets the fixed data for a DataWindow when the DataWindow closes. Once a DataWindow closes none of its
     * AggregatorData changes.
     *
     * @param data the fixed data for a DataWindow on close.
     */
    @VisibleForTesting
    void setDataAndClose(Map<Aggregator, AggregatorData> data) {
      this.data = data;
    }

    /**
     * Indicates if the DataWindow is closed.
     *
     * @return <b>TRUE</b> if it is closed, <b>FALSE</b> if it is not.
     */
    boolean isClosed() {
      return data != null;
    }
  }

  private final Set<Aggregator> aggregators;
  private volatile Map<Aggregator, AggregatorData> data;
  private EvictingQueue<DataWindow> dataWindowQueue;
  private volatile List<DataWindow> dataWindowList;
  private DataWindow currentDataWindow;
  private boolean started;
  private boolean stopped;

  /**
   * Creates an AggregatorDataProvider for a family of Aggregators that will close data windows together (atomically)
   *
   * @param windowsToKeep number of data windows to keep in memory, including the live one.
   */
  public AggregatorDataProvider(int windowsToKeep) {
    Utils.checkArgument(windowsToKeep > 0, "windows to keep must be greater than zero");
    aggregators = new HashSet<>();
    dataWindowQueue = EvictingQueue.create(windowsToKeep);
    dataWindowList = Collections.emptyList();
  }

  @VisibleForTesting
  DataWindow createDataWindow(long endTimeMillis) {
    return new DataWindow(endTimeMillis);
  }

  /**
   * Adds an Aggregator to the AggregatorDataProvider.
   * <p/>
   * Top level Aggregators can be added only if the AggregatorDataProvider <b>has not</b> been started yet.
   * <p/>
   * Group-by element Aggregators can be added only  if the AggregatorDataProvider <b>has</b> been started yet.
   *
   * @param aggregator aggregator to add
   */
  public void addAggregator(Aggregator aggregator) {
    if (aggregator.getGroupByParent() == null) {
      Utils.checkState(!started, "Already started");
      aggregators.add(aggregator);
    } else {
      Utils.checkState(started, "Not started");
      Map<String, Aggregator> children = currentDataWindow.getAllGroupByElementAggregators()
                                                          .computeIfAbsent(aggregator.getGroupByParent(),
                                                              aggregator1 -> new ConcurrentHashMap<>()
                                                          );
      children.put(aggregator.getName(), aggregator);
    }
  }


  /**
   * Starts the AggregatorDataProvider instance.
   *
   * @param newDataWindowEndTimeMillis ending time of the new DataWindow.
   */
  public void start(long newDataWindowEndTimeMillis) {
    started = true;
    roll(newDataWindowEndTimeMillis);
  }

  /**
   * Stops the AggregatorDataProvider instance.
   */
  public void stop() {
    Utils.checkState(started, "Not started");
    Utils.checkState(!stopped, "Already stopped");
    stopped = true;
  }

  /**
   * Atomically rolls the DataWindow of all aggregators associated with the AggregatorDataProvider.
   *
   * @param newDataWindowEndTimeMillis ending time of the new DataWindow.
   * @return a Map with all the Aggregators data of the DataWindow that closed.
   */
  public Map<Aggregator, AggregatorData> roll(long newDataWindowEndTimeMillis) {
    Utils.checkState(started, "Not started");
    Utils.checkState(!stopped, "Already stopped");

    Map<Aggregator, AggregatorData> oldData = data;
    Map<Aggregator, AggregatorData> newData = new ConcurrentHashMap<>();
    for (Aggregator aggregator : aggregators) {
      newData.put(aggregator, aggregator.createAggregatorData(newDataWindowEndTimeMillis));
    }
    data = newData;

    if (currentDataWindow != null) {
      currentDataWindow.setDataAndClose(oldData);
    }
    DataWindow newDataWindow = createDataWindow(newDataWindowEndTimeMillis);
    synchronized (dataWindowQueue) {
      dataWindowQueue.add(newDataWindow);
      dataWindowList = new ArrayList<>(dataWindowQueue);
    }
    currentDataWindow = newDataWindow;
    return oldData;
  }

  /**
   * Returns all the Aggregators data of the current DataWindow.
   *
   * @return all the Aggregators data of the current DataWindow.
   */
  public Map<Aggregator, AggregatorData> get() {
    Utils.checkState(started, "Not started");
    return data;
  }

  /**
   * Returns all the DataWindows (in order, oldest first, live one last) remembered by the AggregatorDataProvider.
   *
   * @return all the DataWindows (in order, oldest first, live one last) remembered by the AggregatorDataProvider.
   */
  public List<DataWindow> getDataWindows() {
    return dataWindowList;
  }

  /**
   * Returns the current AggregatorData for an Agregator.
   * <p/>
   * This method is also used by the group-by element Aggregators.
   *
   * @param aggregator Aggregator to get the AggregatorData for.
   * @return the current AggregatorData for an Agregator.
   */
  public AggregatorData getData(Aggregator aggregator) {
    Utils.checkState(started, "Not started");
    Utils.checkState(!stopped, "Already stopped");
    Utils.checkNotNull(aggregator, "aggregator");
    // only groupBy group aggregators are created on the fly for simple aggregators they are created on roll.
    Utils.checkArgument((aggregator.getGroupByParent() == null && aggregators.contains(aggregator)) ||
                        (aggregator.getGroupByParent() != null && aggregators.contains(aggregator.getGroupByParent())),
        Utils.formatL("Agregator {} is not registered to provider, nor its parent", aggregator)
    );
    return data.computeIfAbsent(
        aggregator,
        aggregator1 -> aggregator1.createAggregatorData(currentDataWindow.getEndTimeMillis())
    );
  }

}
