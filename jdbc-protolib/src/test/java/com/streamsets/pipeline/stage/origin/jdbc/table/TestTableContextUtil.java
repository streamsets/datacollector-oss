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

import com.streamsets.pipeline.lib.jdbc.multithread.DatabaseVendor;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContextUtil;
import org.junit.Test;

import java.sql.Types;
import java.util.Collections;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestTableContextUtil {

  @Test
  public void testIsTimestampWithNanosFormat() {
    String timestampGoodFormat = "1107867600100<n>105000";
    String timestampWrongFormat_1 = "1107867600100<n>1050<n>00";
    String timestampWrongFormat_2 = "1107867600100";
    String timestampWrongFormat_3 = "105000";
    String timestampWrongFormat_4 = "1107867600100<n>";
    String timestampWrongFormat_5 = "<n>105000";

    assertTrue(TableContextUtil.isTimestampWithNanosFormat(timestampGoodFormat));
    assertFalse(TableContextUtil.isTimestampWithNanosFormat(timestampWrongFormat_1));
    assertFalse(TableContextUtil.isTimestampWithNanosFormat(timestampWrongFormat_2));
    assertFalse(TableContextUtil.isTimestampWithNanosFormat(timestampWrongFormat_3));
    assertFalse(TableContextUtil.isTimestampWithNanosFormat(timestampWrongFormat_4));
    assertFalse(TableContextUtil.isTimestampWithNanosFormat(timestampWrongFormat_5));
  }

  @Test
  public void partitionSizeValidation() {
    assertPartitionSize(Types.INTEGER, "1", true);
    assertPartitionSize(Types.INTEGER, "1.6", false);
    assertPartitionSize(Types.INTEGER, "", false);
    assertPartitionSize(Types.DATE, "1", true);
    assertPartitionSize(Types.TIMESTAMP, "1", true);
    assertPartitionSize(Types.TIMESTAMP, "1000", true);
    assertPartitionSize(Types.DECIMAL, "1000", true);
    assertPartitionSize(Types.DECIMAL, "90.5", true);
    assertPartitionSize(Types.DECIMAL, "1.56E+2", true);
    assertPartitionSize(Types.DOUBLE, "90.5", true);
    assertPartitionSize(Types.FLOAT, "90.5", true);
    assertPartitionSize(Types.REAL, "42", true);
    assertPartitionSize(Types.NUMERIC, "1.995", true);
  }

  private static void assertPartitionSize(int sqlType, String partitionSize, boolean valid) {
    assertThat(
        TableContextUtil.getPartitionSizeValidationError(DatabaseVendor.UNKNOWN, sqlType, "col", partitionSize),
        valid ? nullValue() : notNullValue()
    );
  }

  @Test
  public void offsetsBeyondMaxValues() {
    final String offsetCol = "col";

    final boolean first = TableContextUtil.allOffsetsBeyondMaxValues(
        DatabaseVendor.UNKNOWN,
        Collections.singletonMap(offsetCol, Types.INTEGER),
        Collections.singletonMap(offsetCol, "0"),
        Collections.singletonMap(offsetCol, "100000")
    );
    assertFalse(first);

    final boolean second = TableContextUtil.allOffsetsBeyondMaxValues(
        DatabaseVendor.UNKNOWN,
        Collections.singletonMap(offsetCol, Types.BIGINT),
        Collections.singletonMap(offsetCol, "14000000"),
        Collections.singletonMap(offsetCol, "13999999")
    );
    assertTrue(second);
  }

  @Test
  public void testGetQuotedQualifiedTableName() {
    assertEquals("'s'.'t'", TableContextUtil.getQuotedQualifiedTableName("s", "t", "'"));
    assertEquals("'t'", TableContextUtil.getQuotedQualifiedTableName(null, "t", "'"));
  }
}
