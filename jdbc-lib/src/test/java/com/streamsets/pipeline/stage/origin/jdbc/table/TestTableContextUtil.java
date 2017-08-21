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

import com.streamsets.pipeline.lib.jdbc.multithread.TableContextUtil;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

public class TestTableContextUtil {
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
        TableContextUtil.getPartitionSizeValidationError(sqlType, "col", partitionSize),
        valid ? nullValue() : notNullValue()
    );
  }
}
