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
package com.streamsets.pipeline.lib.jdbc;

import com.streamsets.pipeline.lib.jdbc.multithread.BatchTableStrategy;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JdbcTableUtil {
  public static Map<Integer, Integer> decideMaxTableSlotsForThreads(BatchTableStrategy batchTableStrategy, int numberOfThreads, int totalNumberOfTables) {
    Map<Integer, Integer> threadNumberToMaxQueueSize = new HashMap<>();
    if (batchTableStrategy == BatchTableStrategy.SWITCH_TABLES) {
      //If it is switch table strategy, we equal divide the work between all threads
      //(and if it cannot be equal distribute the remaining table slots to subset of threads)
      //int totalNumberOfTables = allTableContexts.size();
      int balancedQueueSize = totalNumberOfTables / numberOfThreads;
      //first divide total tables / number of threads to get
      //an exact balanced number of table slots to be assigned to all threads
      IntStream.range(0, numberOfThreads).forEach(
          threadNumber -> threadNumberToMaxQueueSize.put(threadNumber, balancedQueueSize)
      );
      //Remaining table slots which are not assigned, can be assigned to a subset of threads
      int toBeAssignedTableSlots = totalNumberOfTables % numberOfThreads;

      //Randomize threads and pick a set of threads for processing extra slots
      List<Integer> threadNumbers = IntStream.range(0, numberOfThreads).boxed().collect(Collectors.toList());
      Collections.shuffle(threadNumbers);
      threadNumbers = threadNumbers.subList(0, toBeAssignedTableSlots);

      //Assign the remaining table slots to thread by incrementing the max table slot for each of the randomly selected
      //thread by 1
      for (int threadNumber : threadNumbers) {
        threadNumberToMaxQueueSize.put(threadNumber, threadNumberToMaxQueueSize.get(threadNumber) + 1);
      }
    } else {
      //Assign one table slot to each thread if the strategy is process all available rows
      //So each table will pick up one table process it completely then return it back to pool
      //then pick up a new table and work on it.
      IntStream.range(0, numberOfThreads).forEach(
          threadNumber -> threadNumberToMaxQueueSize.put(threadNumber, 1)
      );
    }
    return threadNumberToMaxQueueSize;
  }

}
