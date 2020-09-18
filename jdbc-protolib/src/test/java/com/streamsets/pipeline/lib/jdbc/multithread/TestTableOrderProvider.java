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
package com.streamsets.pipeline.lib.jdbc.multithread;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.stage.origin.jdbc.table.PartitioningMode;
import com.streamsets.pipeline.stage.origin.jdbc.table.QuoteChar;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableConfigBean;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Parameterized.class)
@PrepareForTest( {
    Connection.class,
    JdbcUtil.class,
    TestTableOrderProvider.class
})
@PowerMockIgnore({
    "jdk.internal.reflect.*"
})
public class TestTableOrderProvider {
  private static final Logger LOG = LoggerFactory.getLogger(TestTableOrderProvider.class);
  private static final Joiner JOINER = Joiner.on(",");

  //                 --- b ----
  //                 |         |
  //                \|/       \|/
  //                 c       --a ----> f --> e
  //                 |      |         /|\
  //                 |     \|/         |
  //                  ----> d ---------
  private static final Multimap<String, String> REFERRED_TABLE_MAP_WITHOUT_CYCLES =
      new ImmutableMultimap.Builder<String, String>()
          // a refers b (so edge from b -> a suggesting b should be picked for ingest first)
          .put("a", "b")
          .put("c", "b")
          .put("d", "a")
          .put("d", "c")
          .put("f", "a")
          .put("f", "d")
          .put("e", "f")
          .build();

  //                 ---> a ----
  //                 |          |
  //                 |         \|/
  //                 d          b
  //                /|\         |
  //                 |          |
  //                  ---- c <--

  private static final Multimap<String, String> REFERRED_TABLE_MAP_WITH_CYCLES =
      new ImmutableMultimap.Builder<String, String>()
          .put("a", "b")
          .put("b", "c")
          .put("c", "d")
          .put("d", "a")
          .build();

  // c-->a<-->a-->b
  private static final Multimap<String, String> REFERRED_TABLE_MAP_WITH_SELF_CYCLES =
      new ImmutableMultimap.Builder<String, String>()
          .put("b", "a")
          .put("a", "a")
          .put("a", "c")
          .build();

  private enum ReferredTablesTestRelation {
    NO_CYCLES(REFERRED_TABLE_MAP_WITHOUT_CYCLES, ImmutableList.of("e", "f", "a", "d", "c", "b")),
    WITH_CYCLES(REFERRED_TABLE_MAP_WITH_CYCLES, ImmutableList.of("b", "c", "d", "a")),
    WITH_SELF_CYCLES(REFERRED_TABLE_MAP_WITH_SELF_CYCLES, ImmutableList.of("c", "b", "a")),
    ;

    final Multimap<String, String> referredTableMap;
    final List<String> tableListingOrder;

    ReferredTablesTestRelation(Multimap<String, String> referredTableMap, List<String> tableListingOrder) {
      this.referredTableMap = referredTableMap;
      this.tableListingOrder = tableListingOrder;
    }

    public Multimap<String,String> getReferredTableMap() {
      return referredTableMap;
    }

    public List<String> getTableListingOrder() {
      return tableListingOrder;
    }
  }

  @Parameterized.Parameters(name = "Table Order Strategy : {0}, Test Type : {1}")
  public static Collection<Object[]> data() throws Exception {
    return Arrays.asList(new Object[][]{
        {TableOrderStrategy.NONE, ReferredTablesTestRelation.NO_CYCLES, ImmutableList.of("e", "f", "a", "d", "c", "b")},
        {TableOrderStrategy.NONE, ReferredTablesTestRelation.WITH_CYCLES, ImmutableList.of("b", "c", "d", "a")},
        {TableOrderStrategy.NONE, ReferredTablesTestRelation.WITH_SELF_CYCLES, ImmutableList.of("c", "b", "a")},

        {TableOrderStrategy.ALPHABETICAL, ReferredTablesTestRelation.NO_CYCLES, ImmutableList.of("a", "b", "c", "d", "e", "f")},
        {TableOrderStrategy.ALPHABETICAL, ReferredTablesTestRelation.WITH_CYCLES, ImmutableList.of("a", "b", "c", "d")},
        {TableOrderStrategy.ALPHABETICAL, ReferredTablesTestRelation.WITH_SELF_CYCLES, ImmutableList.of("a", "b", "c")},

        {TableOrderStrategy.REFERENTIAL_CONSTRAINTS, ReferredTablesTestRelation.NO_CYCLES, ImmutableList.of("b", "a", "c", "d", "f", "e")},
        {TableOrderStrategy.REFERENTIAL_CONSTRAINTS, ReferredTablesTestRelation.WITH_CYCLES, null},
        {TableOrderStrategy.REFERENTIAL_CONSTRAINTS, ReferredTablesTestRelation.WITH_SELF_CYCLES, ImmutableList.of("c", "a", "b")},
    });
  }

  private final TableOrderStrategy tableOrderStrategy;
  private final ReferredTablesTestRelation referredTablesTestRelation;
  private final List<String> expectedOrderOrNullIfError;

  private TableOrderProvider tableOrderProvider;
  private Map<String, TableContext> tableContextsMap;

  public TestTableOrderProvider(TableOrderStrategy tableOrderStrategy, ReferredTablesTestRelation referredTablesTestRelation, List<String> expectedOrderOrNullIfError) {
    this.tableOrderStrategy = tableOrderStrategy;
    this.referredTablesTestRelation = referredTablesTestRelation;
    this.expectedOrderOrNullIfError = expectedOrderOrNullIfError;
    tableContextsMap = new LinkedHashMap<>();
  }

  private static TableContext getTableContext(String tableName) {
    return new TableContext(
        DatabaseVendor.UNKNOWN,
        QuoteChar.NONE,
        null,
        tableName,
        new LinkedHashMap<>(ImmutableMap.of("prim_key", Types.INTEGER)),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        TableConfigBean.ENABLE_NON_INCREMENTAL_DEFAULT_VALUE,
        PartitioningMode.DISABLED,
        -1,
        null
    );
  }

  @Before
  public void setup() throws Exception {
    PowerMockito.replace(
        MemberMatcher.method(
            JdbcUtil.class,
            "getReferredTables",
            Connection.class,
            String.class,
            String.class
        )
    ).with((proxy, method, args) -> new HashSet<>(referredTablesTestRelation.getReferredTableMap().get((String)args[2])));
    tableOrderProvider = new TableOrderProviderFactory(PowerMockito.mock(Connection.class), tableOrderStrategy).create();
    for (String table : referredTablesTestRelation.getTableListingOrder()) {
      final TableContext tableContext = getTableContext(table);
      tableContextsMap.put(table, tableContext);
    }
  }


  @Test
  @SuppressWarnings("unchecked")
  public void testTableOrderStrategy() throws Exception {
    List<String> expectedStrategyOrderedTables = expectedOrderOrNullIfError;
    if (expectedOrderOrNullIfError == null) {
      try {
        tableOrderProvider.initialize(tableContextsMap);
        Assert.fail("Table Order Provider initialization should have failed");
      } catch (StageException e) {
        //Expected
        LOG.info("Expected Error:", e);
      }
    } else {
      tableOrderProvider.initialize(tableContextsMap);
      Map<String, TableContext> tableContextMap = Whitebox.getInternalState(tableOrderProvider, "tableContextMap");

      Multimap<String, String> referredTablesMap = referredTablesTestRelation.getReferredTableMap();
      int totalNumberOfTables = Sets.union(referredTablesMap.keySet(), new HashSet<>(referredTablesMap.values())).size();
      Assert.assertEquals(totalNumberOfTables, tableContextMap.size());

      List<String> actualOrder = new LinkedList<>(tableOrderProvider.getOrderedTables());

      LOG.debug("Expected Order: {}", JOINER.join(expectedOrderOrNullIfError));
      LOG.debug("Actual Order: {}", JOINER.join(actualOrder));

      for (int i = 0; i < expectedStrategyOrderedTables.size(); i++) {
        String expectedTable = expectedStrategyOrderedTables.get(i);
        String actualTable = actualOrder.get(i);
        Assert.assertEquals("Table order seems to be wrong", expectedTable, actualTable);
      }
    }
  }
}
