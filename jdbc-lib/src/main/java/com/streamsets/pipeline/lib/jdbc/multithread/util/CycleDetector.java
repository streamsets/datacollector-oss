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

import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * Detects the cycle in the {@link DirectedGraph}
 * @param <V> Type of the vertices of the {@link DirectedGraph}
 */
public final class CycleDetector<V> {
  private final DirectedGraph<V> directedGraph;

  public CycleDetector(DirectedGraph<V> directedGraph) {
    this.directedGraph = directedGraph;
  }

  /**
   * Find cycles in the {@link #directedGraph}.
   * @return Set of list of vertices involved in the cycle.
   */
  boolean isGraphCyclic() {
    for (V vertex : directedGraph.vertices()) {
      boolean areThereCycles = findCycles(new LinkedHashSet<V>(), vertex);
      if (areThereCycles) {
        return true;
      }
    }
    return false;
  }

  private boolean findCycles(LinkedHashSet<V> visitedVertices, V currentVertex) {
    //Seen the vertex twice
    if (!visitedVertices.add(currentVertex)) {
      return true;
    }
    LinkedHashSet<V> visitedVerticesForTheCurrentVertex = new LinkedHashSet<>(visitedVertices);
    Collection<V> outwardEdgeVertices = directedGraph.getOutwardEdgeVertices(currentVertex);
    for (V outwardEdgeVertex : outwardEdgeVertices) {
      boolean areThereCycles = findCycles(visitedVerticesForTheCurrentVertex, outwardEdgeVertex);
      if (areThereCycles) {
        return true;
      }
    }
    return false;
  }
}
