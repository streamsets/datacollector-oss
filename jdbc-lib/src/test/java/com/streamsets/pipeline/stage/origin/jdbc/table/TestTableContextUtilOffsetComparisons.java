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
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.streamsets.pipeline.lib.jdbc.multithread.DatabaseVendor;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContextUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static com.streamsets.testing.Matchers.hasSameSignAs;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class TestTableContextUtilOffsetComparisons {

  private final DatabaseVendor databaseVendor;
  private final int offsetJdbcType;
  private final String leftOffset;
  private final String rightOffset;
  private final int comparisonResult;

  public TestTableContextUtilOffsetComparisons(
      DatabaseVendor databaseVendor,
      int offsetJdbcType,
      String leftOffset,
      String rightOffset,
      int comparisonResult
  ) {
    this.databaseVendor = databaseVendor;
    this.offsetJdbcType = offsetJdbcType;
    this.leftOffset = leftOffset;
    this.rightOffset = rightOffset;
    this.comparisonResult = comparisonResult;
  }

  @Parameterized.Parameters(
      name = "Database vendor: {0}, Offset JDBC Type: {1}, Left Offset Value: {2}, Right Offset Value: {3}," +
          " Expected Comparison Result: {4}"
  )
  public static Collection<Object[]> data() {
    final List<Object[]> data = new LinkedList<>();
    data.add(new Object[] {DatabaseVendor.UNKNOWN, Types.INTEGER, "-5", "18", -1});
    data.add(new Object[] {DatabaseVendor.UNKNOWN, Types.BIGINT, "-999", "-1000", 1});
    data.add(new Object[] {DatabaseVendor.UNKNOWN, Types.FLOAT, "18.4", "18.4", 0});
    data.add(new Object[] {DatabaseVendor.UNKNOWN, Types.DOUBLE, "984.078", "-93.837", 1});
    data.add(new Object[] {DatabaseVendor.UNKNOWN, Types.TIMESTAMP, "1559184471", "1559184473", -1});
    data.add(new Object[] {
        DatabaseVendor.UNKNOWN,
        Types.DECIMAL,
        "18442.8849904",
        "18442.8849904",
        0
    });
    data.add(new Object[] {
        DatabaseVendor.ORACLE,
        TableContextUtil.TYPE_ORACLE_TIMESTAMP_WITH_TIME_ZONE,
        "2011-12-03T10:15:30+01:00",
        "2011-12-04T11:17:30+05:00",
        -1
    });
    return data;
  }

  @Test
  public void testComparison() {
    final int result = TableContextUtil.compareOffsetValues(
        offsetJdbcType,
        databaseVendor,
        leftOffset,
        rightOffset
    );

    assertThat(result, hasSameSignAs(comparisonResult));
  }
}
