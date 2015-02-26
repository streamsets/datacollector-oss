/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.Errors;
import com.streamsets.pipeline.lib.io.CountingReader;
import com.streamsets.pipeline.lib.xml.StreamingXmlParser;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.List;

public class XmlRecordCreator implements RecordCreator {

  private final Source.Context context;
  private final String topic;

  public XmlRecordCreator(Source.Context context, String topic) {
    this.topic = topic;
    this.context = context;
  }

  @Override
  public List<Record> createRecords(MessageAndOffset message, int currentRecordCount) throws StageException {
    Record record = RecordCreatorUtil.createRecord(context, topic, message.getPartition(), currentRecordCount++);
    try (CountingReader reader =
           new CountingReader(new BufferedReader(new InputStreamReader(
             new ByteArrayInputStream(message.getPayload()))))) {
      StreamingXmlParser xmlParser = new StreamingXmlParser(reader);
      record.set(xmlParser.read());
      return ImmutableList.of(record);
    } catch (Exception e) {
      throw new StageException(Errors.KAFKA_01, e.getMessage(), e);
    }
  }
}
