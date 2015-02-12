/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;

import java.util.List;

public class LogRecordCreator implements RecordCreator {
  private static final String DOT = ".";

  private final Source.Context context;
  private final String topic;

  public LogRecordCreator(Source.Context context, String topic) {
    this.topic = topic;
    this.context = context;
  }

  @Override
  public List<Record> createRecords(MessageAndOffset message, int currentRecordCount) throws StageException {
    Record record = RecordCreatorUtil.createRecord(context, topic, message.getPartition(), currentRecordCount++);
    byte[] payload = message.getPayload();
    Field field;
    if(payload == null) {
      field = Field.create(Field.Type.STRING, payload);
    } else {
      field = Field.create(new String(payload));
    }
    record.set(field);
    return ImmutableList.of(record);
  }
}
