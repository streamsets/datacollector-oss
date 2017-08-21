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

import org.junit.Assert;
import org.junit.Test;

public class TestCycleDetector {

  @Test
  public void testNoCycle() throws Exception {
    DirectedGraph<String> directedGraph = new DirectedGraph<>();
    Assert.assertTrue(directedGraph.addVertex("a"));
    Assert.assertTrue(directedGraph.addVertex("b"));
    Assert.assertTrue(directedGraph.addVertex("c"));
    Assert.assertTrue(directedGraph.addVertex("d"));

    directedGraph.addDirectedEdge("a", "b");
    directedGraph.addDirectedEdge("b", "c");
    directedGraph.addDirectedEdge("c", "d");

    CycleDetector<String> cycleDetector = new CycleDetector<>(directedGraph);
    Assert.assertFalse(cycleDetector.isGraphCyclic());
  }


  @Test
  public void testSelfLoop() throws Exception {
    DirectedGraph<String> directedGraph = new DirectedGraph<>();
    Assert.assertTrue(directedGraph.addVertex("a"));
    Assert.assertTrue(directedGraph.addVertex("b"));
    Assert.assertTrue(directedGraph.addVertex("c"));
    Assert.assertTrue(directedGraph.addVertex("d"));

    directedGraph.addDirectedEdge("a", "a");
    directedGraph.addDirectedEdge("a", "b");
    directedGraph.addDirectedEdge("b", "c");
    directedGraph.addDirectedEdge("c", "d");

    CycleDetector<String> cycleDetector = new CycleDetector<>(directedGraph);
    Assert.assertTrue(cycleDetector.isGraphCyclic());
  }

  //  a -> b -> c -> d -
  //  ^                 |
  //  |                 |
  //   -----------------
  @Test
  public void testSingleLoop() throws Exception {
    DirectedGraph<String> directedGraph = new DirectedGraph<>();
    Assert.assertTrue(directedGraph.addVertex("a"));
    Assert.assertTrue(directedGraph.addVertex("b"));
    Assert.assertTrue(directedGraph.addVertex("c"));
    Assert.assertTrue(directedGraph.addVertex("d"));

    directedGraph.addDirectedEdge("a", "b");
    directedGraph.addDirectedEdge("b", "c");
    directedGraph.addDirectedEdge("c", "d");
    directedGraph.addDirectedEdge("d", "a");

    CycleDetector<String> cycleDetector = new CycleDetector<>(directedGraph);
    Assert.assertTrue(cycleDetector.isGraphCyclic());
  }

  //  a -> b -> c -> d ----> e -> f --> g --> h
  //  ^                 |    ^                 |
  //  |                 |    |                 |
  //   -----------------      -----------------
  @Test
  public void testMultipleLoops() throws Exception {
    DirectedGraph<String> directedGraph = new DirectedGraph<>();
    Assert.assertTrue(directedGraph.addVertex("a"));
    Assert.assertTrue(directedGraph.addVertex("b"));
    Assert.assertTrue(directedGraph.addVertex("c"));
    Assert.assertTrue(directedGraph.addVertex("d"));

    directedGraph.addDirectedEdge("a", "b");
    directedGraph.addDirectedEdge("b", "c");
    directedGraph.addDirectedEdge("c", "d");
    directedGraph.addDirectedEdge("d", "a");
    directedGraph.addDirectedEdge("d", "e");

    directedGraph.addDirectedEdge("e", "f");
    directedGraph.addDirectedEdge("f", "g");
    directedGraph.addDirectedEdge("g", "h");
    directedGraph.addDirectedEdge("h", "e");

    CycleDetector<String> cycleDetector = new CycleDetector<>(directedGraph);
    Assert.assertTrue(cycleDetector.isGraphCyclic());
  }
}
