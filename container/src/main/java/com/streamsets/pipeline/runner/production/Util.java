/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

public class Util<T> {

  public List<T> merge(int n, List<LinkedList<T>> listsToBeMerged, Comparator<T> comparator) {
    List<T> result = new ArrayList<>(n);

    PriorityQueue<T> priorityQueue = new PriorityQueue<>(listsToBeMerged.size());

    //populate priority queue

    return result;
  }
}
