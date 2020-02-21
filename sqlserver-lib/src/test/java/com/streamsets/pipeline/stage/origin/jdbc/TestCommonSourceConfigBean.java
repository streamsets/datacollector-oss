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

package com.streamsets.pipeline.stage.origin.jdbc;

import org.junit.Test;

import java.math.BigDecimal;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TestCommonSourceConfigBean {

  @Test
  public void testRateLimitUpgradeCalculations() {
    // simple case; one query with sleep time of 1 with 1 thread
    BigDecimal val1 = CommonSourceConfigBean.getQueriesPerSecondFromInterval("1", 1, "", "");
    assertEquals(val1.doubleValue(), 1.0, 0.0);

    // two threads sleeping 5 seconds is 0.4 queries per second
    BigDecimal val2 = CommonSourceConfigBean.getQueriesPerSecondFromInterval(5, 2, "", "");
    assertThat(val2, equalTo(new BigDecimal("0.4")));

    // fewer than 1 threads should be treated as 1
    BigDecimal val3 = CommonSourceConfigBean.getQueriesPerSecondFromInterval(1, 0, "", "");
    assertEquals(val3.doubleValue(), 1.0, 0.0);

    // an interval of 0 corresponds to 0 per second, which means unlimited
    BigDecimal val4 = CommonSourceConfigBean.getQueriesPerSecondFromInterval("${0 * SECONDS}", 4, "", "");
    assertThat(val4, equalTo(BigDecimal.ZERO));
  }
}
