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
package com.streamsets.pipeline.lib.jdbc.multithread.util;

import com.streamsets.pipeline.api.impl.Utils;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * A Topological Sorter for a {@link DirectedGraph}. The graph should be acyclic or an exception is thrown.
 * Takes a {@link #tieComparator} for breaking ties between vertices with same position in the order
 * @param <V> Type of Vertices
 */
public final class TopologicalSorter<V> {
  private final DirectedGraph<V> directedGraph;
  private final Comparator<V> tieComparator;

  public TopologicalSorter(DirectedGraph<V> directedGraph, Comparator<V> tieComparator) {
    this.directedGraph = directedGraph;
    this.tieComparator = tieComparator;
  }

  /**
   * Sorts the vertices in the topological order using {@link #tieComparator} if needed to break ties.
   * Throws {@link IllegalStateException} if there is a cycle detected.
   *
   * @return Topological sorted order for the @{@link DirectedGraph}
   */
  public SortedSet<V> sort() {
    final Map<V, Integer> vertexToSortedNumber = new HashMap<>();
    Map<V, Integer> inEdgesCount = new TreeMap<>();

    SortedSet<V> sortedSet = new TreeSet<>((o1, o2) -> {
      Integer sortedNumber1 = vertexToSortedNumber.get(o1);
      Integer sortedNumber2 = vertexToSortedNumber.get(o2);
      if (sortedNumber1.intValue() == sortedNumber2.intValue()) {
        //If there is no tie comparator and there is a tie, arrange o1 before o2.
        return (tieComparator != null) ? tieComparator.compare(o1, o2) : -1;
      }
      return sortedNumber1.compareTo(sortedNumber2);
    });


    directedGraph.vertices().forEach(vertex -> {
      Collection<V> inwardVertices = directedGraph.getInwardEdgeVertices(vertex);
      inEdgesCount.put(vertex, inwardVertices.size());
    });

    Queue<V> currentVertices = inEdgesCount.keySet()
                                           .stream()
                                           .filter(v -> inEdgesCount.get(v) == 0)
                                           .collect(Collectors.toCollection(LinkedList::new));


    final AtomicInteger order = new AtomicInteger(0);

    while(!currentVertices.isEmpty()) {
      V currentVertex = currentVertices.poll();
      vertexToSortedNumber.put(currentVertex, order.getAndIncrement());
      sortedSet.add(currentVertex);
      directedGraph.getOutwardEdgeVertices(currentVertex).forEach(
          neighbor -> {
            int inDegree = inEdgesCount.get(neighbor);
            if(--inDegree == 0) {
              currentVertices.add(neighbor);
            }
            inEdgesCount.put(neighbor, inDegree);
          }
      );
    }
    // If our resulting topological ordering does not contain all the vertices it necessarily means that
    // there is a cycle
    Utils.checkState(sortedSet.size() == directedGraph.vertices().size(), "Graph contains cycles");
    return sortedSet;
  }

}
