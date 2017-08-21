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

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class TestDirectedGraph {

  @Test
  public void testAddVertices() throws Exception {
    Set<String> toBeAddedVertices = ImmutableSet.of("a", "b", "c");

    DirectedGraph<String> directedGraph = new DirectedGraph<>();
    Assert.assertTrue(directedGraph.addVertex("a"));
    Assert.assertTrue(directedGraph.addVertex("b"));
    Assert.assertTrue(directedGraph.addVertex("c"));

    Assert.assertEquals(3, directedGraph.vertices().size());
    Assert.assertTrue(toBeAddedVertices.containsAll(directedGraph.vertices()));
    Assert.assertTrue(directedGraph.vertices().containsAll(toBeAddedVertices));
  }

  @Test
  public void testAddMultipleTimeSameVertices() throws Exception {
    Set<String> toBeAddedVertices = ImmutableSet.of("a", "b", "c");

    DirectedGraph<String> directedGraph = new DirectedGraph<>();
    Assert.assertTrue(directedGraph.addVertex("a"));
    Assert.assertTrue(directedGraph.addVertex("b"));
    Assert.assertTrue(directedGraph.addVertex("c"));

    Assert.assertFalse(directedGraph.addVertex("a"));
    Assert.assertFalse(directedGraph.addVertex("b"));
    Assert.assertFalse(directedGraph.addVertex("c"));

    Assert.assertEquals(3, directedGraph.vertices().size());
    Assert.assertTrue(toBeAddedVertices.containsAll(directedGraph.vertices()));
    Assert.assertTrue(directedGraph.vertices().containsAll(toBeAddedVertices));
  }

  @Test
  public void testEdges() throws Exception {
    DirectedGraph<String> directedGraph = new DirectedGraph<>();
    Assert.assertTrue(directedGraph.addVertex("a"));
    Assert.assertTrue(directedGraph.addVertex("b"));
    Assert.assertTrue(directedGraph.addVertex("c"));
    Assert.assertTrue(directedGraph.addVertex("d"));

    directedGraph.addDirectedEdge("a", "b");
    directedGraph.addDirectedEdge("a", "c");
    directedGraph.addDirectedEdge("b", "c");
    directedGraph.addDirectedEdge("b", "d");
    directedGraph.addDirectedEdge("c", "d");

    Assert.assertEquals(2, directedGraph.getOutwardEdgeVertices("a").size());
    Assert.assertTrue(directedGraph.getOutwardEdgeVertices("a").containsAll(ImmutableSet.of("b", "c")));
    Assert.assertEquals(0, directedGraph.getInwardEdgeVertices("a").size());

    Assert.assertEquals(2, directedGraph.getOutwardEdgeVertices("b").size());
    Assert.assertTrue(directedGraph.getOutwardEdgeVertices("b").containsAll(ImmutableSet.of("c", "d")));
    Assert.assertEquals(1, directedGraph.getInwardEdgeVertices("b").size());
    Assert.assertTrue(directedGraph.getInwardEdgeVertices("b").containsAll(ImmutableSet.of("a")));

    Assert.assertEquals(1, directedGraph.getOutwardEdgeVertices("c").size());
    Assert.assertTrue(directedGraph.getOutwardEdgeVertices("c").containsAll(ImmutableSet.of("d")));
    Assert.assertEquals(2, directedGraph.getInwardEdgeVertices("c").size());
    Assert.assertTrue(directedGraph.getInwardEdgeVertices("c").containsAll(ImmutableSet.of("a", "b")));

    Assert.assertEquals(0, directedGraph.getOutwardEdgeVertices("d").size());
    Assert.assertEquals(2, directedGraph.getInwardEdgeVertices("d").size());
    Assert.assertTrue(directedGraph.getInwardEdgeVertices("d").containsAll(ImmutableSet.of("b", "c")));
  }

}
