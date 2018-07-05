/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.sqlserver;

import com.streamsets.pipeline.api.Source;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class TestSQLServerCDCSource {
  @Test
  public void testLastHandleOffset() throws Exception {
    SQLServerCDCSource source = new SQLServerCDCSource(null, null, null, null);

    // SDC-9410: when the pipeline starts from SCH, the POLL_SOURCE_OFFSET_KEY passes null which results NPE
    Map<String, String> initialOffset = new HashMap<>();
    initialOffset.put(Source.POLL_SOURCE_OFFSET_KEY, null);
    initialOffset.put("cdc_table1", "");
    initialOffset.put(source.OFFSET_VERSION, source.OFFSET_VERSION_1);

    Map<String, String> cachedOffset = new HashMap<>();

    SQLServerCDCSource mockSource = Mockito.spy(source);
    Mockito.doReturn(cachedOffset).when(mockSource).getOffsets();
    mockSource.handleLastOffset(initialOffset);

    Assert.assertEquals(2, cachedOffset.size());
  }
}
