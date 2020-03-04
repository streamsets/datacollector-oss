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
import java.util.Random;
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

  private int randomInteger(Random rng, int lo, int hi) {
    int r = Math.abs(rng.nextInt());
    int n = hi - lo + 1;
    return (r % n) + lo;
  }

  // Edges are always a -> b such that a < b, so no loop is possible
  private DirectedGraph<Integer> randomDirectedAcyclicGraph(int n, int degreeLo, int degreeHi, int seed) {
    DirectedGraph<Integer> ret = new DirectedGraph<>();
    Random rng = new Random(seed);
    for(int i = 1; i < n; ++i) {
      int degree = randomInteger(rng, degreeLo, degreeHi);
      for(int j = 0; j < degree; ++j) {
        int neighbor = randomInteger(rng, 0, i - 1);
        ret.addDirectedEdge(neighbor, i);
      }
    }
    return ret;
  }

  // Graph from SDC-13273
  @Test
  public void testTopologicalSorterDAG() {
    DirectedGraph<Integer> g = new DirectedGraph<>();

    Assert.assertTrue(g.addVertex(0));
    Assert.assertTrue(g.addVertex(1));
    Assert.assertTrue(g.addVertex(2));
    Assert.assertTrue(g.addVertex(3));
    Assert.assertTrue(g.addVertex(4));

    g.addDirectedEdge(0, 1);
    g.addDirectedEdge(0, 2);
    g.addDirectedEdge(0, 3);
    g.addDirectedEdge(0, 4);

    g.addDirectedEdge(1, 2);
    g.addDirectedEdge(1, 3);
    g.addDirectedEdge(1, 4);

    g.addDirectedEdge(2, 4);

    g.addDirectedEdge(3, 4);

    TopologicalSorter topologicalSorter = new TopologicalSorter(g, Comparator.naturalOrder());

    SortedSet<Integer> order = topologicalSorter.sort();

    int zero = order.first();
    order.remove(zero);

    Assert.assertEquals(0, zero);

    int one = order.first();
    order.remove(one);

    Assert.assertEquals(1, one);

    int two = order.first();
    order.remove(two);

    int three = order.first();
    order.remove(three);

    Assert.assertEquals(2, two);
    Assert.assertEquals(3, three);

    int four = order.first();
    order.remove(four);

    Assert.assertEquals(4, four);
  }


  @Test
  public void testTopologicalSorterBigGraph() {
    DirectedGraph<Integer> g = randomDirectedAcyclicGraph(100000, 0, 20, 1234);
    TopologicalSorter topologicalSorter = new TopologicalSorter(g, (Comparator<Integer>) (a, b) -> 0);
    topologicalSorter.sort();
  }

}
