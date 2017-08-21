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
package com.streamsets.pipeline.stage.lib.kinesis;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.apache.commons.lang3.StringUtils;
import org.powermock.api.mockito.PowerMockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil.ONE_MB;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class KinesisTestUtil {
  public static List<Record> getProducerTestRecords(int i) {
    List<Record> records = new ArrayList<>(i);

    for (int j = 0; j < i; j++) {
      Record record = RecordCreator.create();
      Map<String, Field> root = new HashMap<>();
      root.put("a", Field.create(1));
      root.put("b", Field.create(2));
      root.put("seq", Field.create(j));
      record.set(Field.create(root));
      records.add(record);
    }

    return records;
  }

  public static Record getTooLargeRecord() {
    StringBuilder largeString = new StringBuilder();
    for (int i = 0; i < ONE_MB + 10; i++) {
      largeString.append("a");
    }
    Record record = RecordCreator.create();
    record.set(Field.create(largeString.toString()));
    return record;
  }

  public static List<com.amazonaws.services.kinesis.model.Record> getConsumerTestRecords(int i) {
    List<com.amazonaws.services.kinesis.model.Record> records = new ArrayList<>(i);

    for (int j = 0; j < i; j++) {
      com.amazonaws.services.kinesis.model.Record record = new com.amazonaws.services.kinesis.model.Record()
          .withData(ByteBuffer.wrap(String.format("{\"seq\": %s}", j).getBytes()))
          .withPartitionKey(StringUtils.repeat("0", 19))
          .withSequenceNumber(String.valueOf(j))
          .withApproximateArrivalTimestamp(Calendar.getInstance().getTime());
      records.add(new UserRecord(record));
    }

    return records;
  }

  public static com.amazonaws.services.kinesis.model.Record getBadConsumerTestRecord(int seqNo) {
    com.amazonaws.services.kinesis.model.Record record = new com.amazonaws.services.kinesis.model.Record()
        .withData(ByteBuffer.wrap(String.format("{\"seq\": %s", seqNo).getBytes()))
        .withPartitionKey(StringUtils.repeat("0", 19))
        .withSequenceNumber(String.valueOf(seqNo))
        .withApproximateArrivalTimestamp(Calendar.getInstance().getTime());

    return new UserRecord(record);
  }

  public static com.amazonaws.services.kinesis.model.Record getConsumerTestRecord(int seqNo) {
    com.amazonaws.services.kinesis.model.Record record = new com.amazonaws.services.kinesis.model.Record()
        .withData(ByteBuffer.wrap(String.format("{\"seq\": %s}", seqNo).getBytes()))
        .withPartitionKey(StringUtils.repeat("0", 19))
        .withSequenceNumber(String.valueOf(seqNo))
        .withApproximateArrivalTimestamp(Calendar.getInstance().getTime());

    return new UserRecord(record);
  }

  @SuppressWarnings("unchecked")
  public static void mockKinesisUtil(long numShards) {
    PowerMockito.mockStatic(KinesisUtil.class);

    when(KinesisUtil.checkStreamExists(
        any(ClientConfiguration.class),
        any(KinesisConfigBean.class),
        any(String.class),
        any(List.class),
        any(Stage.Context.class)
        )
    ).thenReturn(numShards);
  }
}
