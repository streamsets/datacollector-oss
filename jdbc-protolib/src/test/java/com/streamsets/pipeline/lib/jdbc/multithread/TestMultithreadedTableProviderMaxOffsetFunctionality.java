/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.lib.jdbc.multithread;

import com.google.common.collect.SortedSetMultimap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestMultithreadedTableProviderMaxOffsetFunctionality extends BaseMultithreadedTableProviderTest {

  /**
   * if true, we will simulate a "late" max row, meaning that a new max value for the offset column will
   * be inserted after the pipeline has started running
   */
  private final boolean simulateLateMaxRow;

  public TestMultithreadedTableProviderMaxOffsetFunctionality(
      boolean simulateLateMaxRow
  ) {
    this.simulateLateMaxRow = simulateLateMaxRow;
  }

  @Parameterized.Parameters(
      name = "Simulate Late Max Row: {0}"
  )
  public static Collection<Object[]> data() {
    final List<Object[]> data = new LinkedList<>();
    data.add(new Object[] { false });
    data.add(new Object[] { true });
    return data;
  }

  @Test
  public void tableNotExhaustedBeforeMax() throws InterruptedException {

    int batchSize = 10;
    String schema = "db";
    String table1Name = "table1";
    String offsetCol = "col";
    String partitionSize = "100";
    int maxActivePartitions = 3;
    int threadNumber = 0;
    int numThreads = 1;

    // suppose the minimum col value is 0
    String minOffsetColValue = "0";
    // and the max is 1000
    String maxOffsetColValue = "1005";
    String updatedMaxOffset = "10001";

    int partitionSequenceForMaxRow = 1 + Integer.parseInt(maxOffsetColValue) / Integer.parseInt(partitionSize);

    TableContext table1 = createTableContext(
        schema,
        table1Name,
        offsetCol,
        partitionSize,
        minOffsetColValue,
        maxOffsetColValue,
        maxActivePartitions,
        true,
        false,
        0l
    );

    MultithreadedTableProvider provider = createTableProvider(
        numThreads,
        table1,
        BatchTableStrategy.PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE,
        tableContext -> {
          if (simulateLateMaxRow) {
            tableContext.updateOffsetColumnToMaxValues(Collections.singletonMap(offsetCol, updatedMaxOffset));
          }
        }
    );

    // should be one partition to start with
    SortedSetMultimap<TableContext, TableRuntimeContext> partitions = provider.getActiveRuntimeContexts();
    assertThat(partitions.size(), equalTo(1));
    assertTrue(partitions.containsKey(table1));

    TableRuntimeContext part1 = partitions.get(table1).first();

    validatePartition(part1, 1, table1, false, false, false, false, offsetCol, partitionSize);

    // simulate a row being processed for the first partition
    part1.recordColumnOffset(offsetCol, "0");
    part1.recordColumnOffset(offsetCol, "1");
    part1.recordColumnOffset(offsetCol, "50");

    assertThat(part1.isAnyOffsetsRecorded(), equalTo(true));
    part1.setResultSetProduced(true);

    validatePartition(part1, 1, table1, true, true, true, false, offsetCol, partitionSize);

    TableRuntimeContext part1Again = provider.nextTable(threadNumber);

    validatePartition(part1Again, 1, table1, true, true, true, false, offsetCol, partitionSize);

    provider.releaseOwnedTable(part1Again, threadNumber);
    TableRuntimeContext part2 = provider.nextTable(threadNumber);
    validatePartition(part2, 2, table1, false, false, true, false, offsetCol, partitionSize);
    provider.releaseOwnedTable(part2, threadNumber);
    TableRuntimeContext part3 = provider.nextTable(threadNumber);
    validatePartition(part3, 3, table1, false, false, true, false, offsetCol, partitionSize);

    // at this point, no partitions should allow a next partition
    assertThat(provider.isNewPartitionAllowed(part1), equalTo(false));
    assertThat(provider.isNewPartitionAllowed(part2), equalTo(false));
    assertThat(provider.isNewPartitionAllowed(part3), equalTo(false));

    // marking the first finished
    provider.reportDataOrNoMoreData(part1, 3, batchSize, true);

    // simulate records for 2nd partition
    part2.recordColumnOffset(offsetCol, "101");
    part2.recordColumnOffset(offsetCol, "105");
    part2.recordColumnOffset(offsetCol, "158");

    // this should now remove the 1st from active contexts...
    assertThat(provider.getActiveRuntimeContexts().values(), not(contains(part1)));
    provider.reportDataOrNoMoreData(part2, batchSize, batchSize, true);
    // however, we should now have a total of partitionSequenceForMaxRow active partitions, because we should now have
    // one with sequence partitionSequenceForMaxRow, and the one right after that (which is the first one whose
    // starting offset value is greater than the max value)
    assertThat(provider.getActiveRuntimeContexts().size(), equalTo(partitionSequenceForMaxRow));

    // but we still can't create any more partitions since we already created through sequence 12
    // (to go past the max offset of 1000)
    assertThat(provider.isNewPartitionAllowed(part3), equalTo(false));

    provider.reportDataOrNoMoreData(part3, 0, batchSize, true);
    provider.releaseOwnedTable(part3, threadNumber);

    // we have just released partition 3, and marked it complete
    // now, cycle through partitions 4-10, marking each of them complete (result set end reached)
    for (int i = 4; i <= partitionSequenceForMaxRow - 1; i++) {
      TableRuntimeContext nextPart = provider.nextTable(threadNumber);
      validatePartition(nextPart, i, table1, false, false, true, false, offsetCol, partitionSize);
      nextPart.setResultSetProduced(true);
      provider.reportDataOrNoMoreData(nextPart, 0, batchSize, true);
      provider.releaseOwnedTable(nextPart, threadNumber);
      // however, because the max column value was greater than the starting position for each of these partitions
      // (1000 > 300, 1000 > 400, etc.), then this table in fact should not be marked finished
      assertThat(provider.getTablesWithNoMoreData(), empty());
      assertThat(provider.shouldGenerateNoMoreDataEvent(), equalTo(false));
    }

    // the next partition should be the last one (i.e. the one that contains the max value)

    TableRuntimeContext lastPart = provider.nextTable(threadNumber);
    validatePartition(lastPart, partitionSequenceForMaxRow, table1, false, false, true, false, offsetCol, partitionSize);

    // simulate reading the last row
    lastPart.recordColumnOffset(offsetCol, maxOffsetColValue);
    assertThat(lastPart.isAnyOffsetsRecorded(), equalTo(true));
    lastPart.setResultSetProduced(true);

    validatePartition(lastPart, partitionSequenceForMaxRow, table1, true, true, true, false, offsetCol, partitionSize);

    // report result set end reached for last partition
    provider.reportDataOrNoMoreData(lastPart, batchSize, batchSize, true);

    // now, obtain up to maxActivePartitions again, in order to truly generate the end event
    // + 1 because we first obtain lastPart (sequence 11) again, before it generates subsequent partitions
    for (int i = 0; i < maxActivePartitions + 1; i++) {
      // until we mark the last partition complete (i.e. partitionSequenceForMaxRow + maxActivePartitions)
      // the no-more-data event should still not be generated
      assertThat(provider.getTablesWithNoMoreData(), empty());
      assertThat(provider.shouldGenerateNoMoreDataEvent(), equalTo(false));
      TableRuntimeContext nextPart = provider.nextTable(threadNumber);
      nextPart.setResultSetProduced(true);
      provider.reportDataOrNoMoreData(nextPart, 0, batchSize, true);
      provider.releaseOwnedTable(nextPart, threadNumber);
    }

    if (simulateLateMaxRow) {
      // a late row was added, with much higher offset value than the original max
      // given that, the no-more-data event should NOT be generated, even though we went through
      // what was previously thought to be the max possible partition sequence
      assertThat(provider.getTablesWithNoMoreData(), empty());
      assertThat(provider.shouldGenerateNoMoreDataEvent(), equalTo(false));
    } else {
      // a late row was not inserted; we should safely be able to generate the no-more-data event
      // at this point
      assertThat(provider.getTablesWithNoMoreData(), hasSize(1));
      assertThat(provider.shouldGenerateNoMoreDataEvent(), equalTo(true));
    }
  }
}
