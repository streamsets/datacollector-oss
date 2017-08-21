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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A Basic directed graph representation which takes a generic {@link V} vertex, the edges do not contain any information
 * NOTE: This is not a thread safe class.
 * @param <V> Type of the vertices of the {@link DirectedGraph}
 */
public final class DirectedGraph<V> {
  private final Multimap<V, V> outwardEdgeVertices;
  private final Multimap<V, V> inwardEdgesVertices;
  private final Set<V> vertices;

  public DirectedGraph() {
    outwardEdgeVertices = HashMultimap.create();
    inwardEdgesVertices = HashMultimap.create();
    vertices = new HashSet<>();
  }

  /**
   * Returns the vertices of the graph
   * @return the vertices of the graph.
   */
  public Set<V> vertices() {
    return vertices;
  }

  /**
   * Returns the outward flowing edge vertices.
   * @param vertex vertex
   * @return the outward flowing edge vertices.
   */
  public Collection<V> getOutwardEdgeVertices(V vertex) {
    Collection<V> outwardEdgeVerticesForVertex =  outwardEdgeVertices.get(vertex);
    return outwardEdgeVerticesForVertex != null ? outwardEdgeVerticesForVertex : Collections.<V>emptySet();
  }

  /**
   * Returns the inward flowing edge vertices.
   * @param vertex vertex
   * @return the inward flowing edge vertices.
   */
  public Collection<V> getInwardEdgeVertices(V vertex) {
    Collection<V> inwardEdgeVerticesForVertex =  inwardEdgesVertices.get(vertex);
    return inwardEdgeVerticesForVertex != null ? inwardEdgeVerticesForVertex : Collections.<V>emptySet();
  }

  /**
   * Add a vertex to this {@link DirectedGraph}
   * @param vertex vertex
   * @return true if vertex is added, false if vertex is already present
   */
  public boolean addVertex(V vertex) {
    return vertices.add(vertex);
  }

  /**
   * Add a directed edge from vertex1 to vertex2 adding those vertices graph (if needed)
   * @param vertex1 vertex1
   * @param vertex2 vertex2
   */
  public void addDirectedEdge(V vertex1, V vertex2) {
    addVertex(vertex1);
    addVertex(vertex2);
    outwardEdgeVertices.put(vertex1, vertex2);
    inwardEdgesVertices.put(vertex2, vertex1);
  }
}
