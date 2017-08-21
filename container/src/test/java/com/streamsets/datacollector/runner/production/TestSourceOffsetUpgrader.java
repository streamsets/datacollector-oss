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
package com.streamsets.datacollector.runner.production;

import com.streamsets.pipeline.api.Source;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestSourceOffsetUpgrader {

  @Test
  public void testUpgradeToV2() {
    SourceOffset offset = new SourceOffset();
    offset.setOffset("one-dimension");

    SourceOffsetUpgrader.upgrade(offset);

    assertEquals(2, offset.getVersion());
    assertNull(offset.getOffset());
    assertNotNull(offset.getOffsets());
    assertEquals(1, offset.getOffsets().size());
    assertTrue(offset.getOffsets().containsKey(Source.POLL_SOURCE_OFFSET_KEY));
    assertEquals("one-dimension", offset.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));
  }

  @Test
  public void testUpgradeNoop() {
    SourceOffset offset = new SourceOffset();
    offset.setOffsets(Collections.singletonMap(Source.POLL_SOURCE_OFFSET_KEY, "one-dimension"));
    offset.setVersion(2);

    SourceOffsetUpgrader.upgrade(offset);

    assertEquals(2, offset.getVersion());
    assertNull(offset.getOffset());
    assertNotNull(offset.getOffsets());
    assertEquals(1, offset.getOffsets().size());
    assertTrue(offset.getOffsets().containsKey(Source.POLL_SOURCE_OFFSET_KEY));
    assertEquals("one-dimension", offset.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));
  }

}
