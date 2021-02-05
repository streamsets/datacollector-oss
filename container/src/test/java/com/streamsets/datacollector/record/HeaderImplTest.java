/*
 * Copyright 2021 StreamSets Inc.
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
package com.streamsets.datacollector.record;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HeaderImplTest {

  @Test
  public void testCopyErrorFrom() {
    Record record = new RecordImpl("stage", "recordID", null, null);
    HeaderImpl originalHeader = (HeaderImpl) record.getHeader();
    originalHeader.setError("stage", "stageName", new ErrorMessage("A", "A", 0));
    originalHeader.setErrorJobName("jobName");
    originalHeader.setErrorJobId("jobId");

    // Blank header
    HeaderImpl copyHeader = new HeaderImpl();
    copyHeader.copyErrorFrom(record);

    Map<String, Object> originalHeaderMap = originalHeader.getInternalHeaderMap();
    Map<String, Object> copyHeaderMap = copyHeader.getInternalHeaderMap();

    for(Map.Entry<String, Object> entry : originalHeaderMap.entrySet()) {
      String key = entry.getKey();
      Object expectedValue = entry.getValue();

      // We want to only compare error headers
      if(!key.startsWith("_.error")) {
        continue;
      }

      // Assert that the headers and values fit
      assertTrue(key, copyHeaderMap.containsKey(key));
      assertEquals(key, expectedValue, copyHeaderMap.get(key));
    }
  }
}
