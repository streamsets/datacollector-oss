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
package com.streamsets.pipeline.stage.destination;

import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class TestToEventTarget {

  @Test
  public void testToEvent() throws Exception {
    TargetRunner runner = new TargetRunner.Builder(ToEventTarget.class)
      .build();

    runner.runInit();
    try {
      Record record = RecordCreator.create();
      record.set(Field.create(Field.Type.STRING, "Catch them all!"));

      runner.runWrite(Collections.singletonList(record));

      List<EventRecord> events = runner.getEventRecords();
      Assert.assertNotNull(events);
      Assert.assertEquals(1, events.size());
      Assert.assertEquals(Field.Type.STRING, events.get(0).get().getType());
      Assert.assertEquals("Catch them all!", events.get(0).get().getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

}
