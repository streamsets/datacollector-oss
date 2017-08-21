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
package ${groupId}.stage.executor.sample;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.ExecutorRunner;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TestSampleExecutor {
  @Test
  public void testWriteSingleRecord() throws Exception {
    ExecutorRunner runner = new ExecutorRunner.Builder(SampleDExecutor.class)
        .addConfiguration("config", "value")
        .build();

    runner.runInit();

    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("first", Field.create("John"));
    fields.put("last", Field.create("Smith"));
    fields.put("someField", Field.create("some value"));
    record.set(Field.create(fields));


    runner.runWrite(Arrays.asList(record));

    // Here check the data destination. E.g. a mock.

    runner.runDestroy();
  }
}
