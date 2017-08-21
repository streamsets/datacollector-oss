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

import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;

public class TestTopologicalSorter {

  //  a -> b -> c -> d -
  //  ^                 |
  //  |                 |
  //   -----------------
  @Test(expected = IllegalStateException.class)
  public void testTopologyWithCycle() throws Exception {
    DirectedGraph<String> directedGraph = new DirectedGraph<>();
    Assert.assertTrue(directedGraph.addVertex("a"));
    Assert.assertTrue(directedGraph.addVertex("b"));
    Assert.assertTrue(directedGraph.addVertex("c"));
    Assert.assertTrue(directedGraph.addVertex("d"));

    directedGraph.addDirectedEdge("a", "b");
    directedGraph.addDirectedEdge("b", "c");
    directedGraph.addDirectedEdge("c", "d");
    directedGraph.addDirectedEdge("d", "a");

    TopologicalSorter<String> topologicalSorter = new TopologicalSorter<>(directedGraph, null);
    topologicalSorter.sort();
  }
  //  a -> b -> c -> d
  @Test
  public void testSimpleTopology() throws Exception {
    DirectedGraph<String> directedGraph = new DirectedGraph<>();
    Assert.assertTrue(directedGraph.addVertex("a"));
    Assert.assertTrue(directedGraph.addVertex("b"));
    Assert.assertTrue(directedGraph.addVertex("c"));
    Assert.assertTrue(directedGraph.addVertex("d"));

    directedGraph.addDirectedEdge("a", "b");
    directedGraph.addDirectedEdge("b", "c");
    directedGraph.addDirectedEdge("c", "d");
    TopologicalSorter<String> topologicalSorter = new TopologicalSorter<>(directedGraph, null);
    Iterator<String> iterator = topologicalSorter.sort().iterator();
    Assert.assertEquals("a", iterator.next());
    Assert.assertEquals("b", iterator.next());
    Assert.assertEquals("c", iterator.next());
    Assert.assertEquals("d", iterator.next());
  }

  //   ----  1 ---          2
  //  |           |         |
  //  |           |         |
  //   -> 3        -> 4 <--   ---> 6
  //      |                        ^
  //       ---------> 5 -----------|
  @Test
  public void testTopologyWithTies() throws Exception {
    DirectedGraph<String> directedGraph = new DirectedGraph<>();
    Assert.assertTrue(directedGraph.addVertex("1"));
    Assert.assertTrue(directedGraph.addVertex("2"));
    Assert.assertTrue(directedGraph.addVertex("3"));
    Assert.assertTrue(directedGraph.addVertex("4"));
    Assert.assertTrue(directedGraph.addVertex("5"));
    Assert.assertTrue(directedGraph.addVertex("6"));

    directedGraph.addDirectedEdge("1", "3");
    directedGraph.addDirectedEdge("1", "4");
    directedGraph.addDirectedEdge("2", "4");
    directedGraph.addDirectedEdge("2", "6");
    directedGraph.addDirectedEdge("3", "5");
    directedGraph.addDirectedEdge("5", "6");

    TopologicalSorter<String> topologicalSorter = new TopologicalSorter<>(directedGraph, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        return o1.compareTo(o2);
      }
    });
    SortedSet<String> topologicalOrder = topologicalSorter.sort();
    Iterator<String> iterator = topologicalOrder.iterator();
    int i = 1;
    while (iterator.hasNext()) {
      String vertex = iterator.next();
      Assert.assertEquals(String.valueOf(i), vertex);
      i++;
    }
  }
}
