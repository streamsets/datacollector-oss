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
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.jdbc.multithread.TableJdbcRunnable;
import com.streamsets.pipeline.lib.jdbc.multithread.TableRuntimeContext;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = {
    TableJdbcSource.class,
    TableJdbcRunnable.class
})
@PowerMockIgnore({
    "javax.security.*",
    "javax.management.*",
    "jdk.internal.reflect.*"
})
public final class ExceptionIT extends BaseTableJdbcSourceIT {
  private static final String EXCEPTION_TABLE = "EXCEPTION_TABLE";
  private static final String EXCEPTION_OFFSET_COLUMN = "EXCEPTION_OFFSET";
  private static final String EXCEPTION_MESSAGE = "Our Own Test Exception";
  private static final String NUM_SQL_ERRORS_FIELD = "numSQLErrors";
  private static final String CREATE_AND_ADD_RECORD_METHOD = "createAndAddRecord";
  private static final String HANDLE_SQL_EXCEPTION_METHOD = "handleSqlException";
  private static final List<Record> EXPECTED_EXCEPTION_TABLE_RECORDS = Lists.newArrayList();


  private static void createRecords(int numberOfTables) {
    IntStream.range(0, numberOfTables).forEach(offsetColumnVal -> {
      Record record = RecordCreator.create();
      record.set(Field.createListMap(new LinkedHashMap<>(ImmutableMap.of(
          EXCEPTION_OFFSET_COLUMN, Field.create(offsetColumnVal)
      ))));
      EXPECTED_EXCEPTION_TABLE_RECORDS.add(record);
    });
  }

  @BeforeClass
  public static void setupTables() throws SQLException {
    try (Statement st = connection.createStatement()) {
      st.addBatch(getCreateStatement(
          database,
          EXCEPTION_TABLE,
          ImmutableMap.of(
              EXCEPTION_OFFSET_COLUMN, FIELD_TYPE_TO_SQL_TYPE_AND_STRING.get(Field.Type.INTEGER)
          ),
          Collections.emptyMap()
      ));

      createRecords(20);

      EXPECTED_EXCEPTION_TABLE_RECORDS.forEach(record -> {
        try {
          st.addBatch(
              getInsertStatement(database, EXCEPTION_TABLE, record.get().getValueAsListMap().values())
          );
        } catch (SQLException e) {
          Throwables.propagate(e);
        }
      });
      st.executeBatch();
    }
  }

  @AfterClass
  public static void deleteTables() throws Exception {
    try (Statement st = connection.createStatement()) {
      st.execute(String.format(DROP_STATEMENT_TEMPLATE, database, EXCEPTION_TABLE));
    }
  }

  @Test
  @Ignore("Investigate/fix sporadic failure (sometimes generates 4 errors intead of expected 4): SDC-6773")
  public void testNumSQLErrors() throws Exception {
    TableConfigBeanImpl tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("%")
        .schema(database)
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        .numSQLErrorRetries(3)
        .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    Map<String, String> offsets = new HashMap<>();

    runner.runInit();

    AtomicInteger exceptionHappenedTimes = new AtomicInteger(0);

    try {
      PowerMockito.replace(
          MemberMatcher.method(
              TableJdbcRunnable.class,
              CREATE_AND_ADD_RECORD_METHOD,
              ResultSet.class,
              TableRuntimeContext.class,
              BatchContext.class
          )
      ).with((proxy, method, args) -> {
        throw new SQLException(EXCEPTION_MESSAGE);
      });


      PowerMockito.replace(
          MemberMatcher.method(
              TableJdbcRunnable.class,
              HANDLE_SQL_EXCEPTION_METHOD,
              SQLException.class)
      ).with((proxy, method, args) -> {
        SQLException sqlException = (SQLException) args[0];
        Assert.assertEquals(EXCEPTION_MESSAGE, sqlException.getMessage());
        int expectedExceptionHappenedTimes = exceptionHappenedTimes.getAndIncrement();
        int actualExceptionHappenedTimes = (Integer) Whitebox.getInternalState(proxy, NUM_SQL_ERRORS_FIELD);
        Assert.assertEquals(expectedExceptionHappenedTimes, actualExceptionHappenedTimes);
        return method.invoke(proxy, args);
      });

      runner.runProduce(offsets, 10, output -> {
        //NOOP
      });

      //Await for runner stop and
      // NOTE: need poll interval to avoid ConcurrentModificationException from StageRunner#getErrors
      Awaitility.await().pollInterval(1, TimeUnit.MILLISECONDS).atMost(Duration.FIVE_MINUTES).until(
          () -> runner.getErrors().size() == 1
      );

      Assert.assertEquals(3, exceptionHappenedTimes.get());
    } finally {
      runner.runDestroy();
    }
  }
}
