/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.io.CountingReader;
import com.streamsets.pipeline.lib.json.OverrunStreamingJsonParser;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;
import com.streamsets.pipeline.lib.util.JsonUtil;
import com.streamsets.pipeline.lib.util.KafkaStageLibError;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;


public class JsonRecordCreator implements RecordCreator {

  private static final String DOT = ".";

  private final StreamingJsonParser.Mode jsonContent;
  private final int maxJsonObjectLen;
  private final Source.Context context;
  private final boolean produceSingleRecord;
  private final String topic;

  public JsonRecordCreator(Source.Context context, StreamingJsonParser.Mode jsonContent, int maxJsonObjectLen,
                           boolean produceSingleRecord, String topic) {
    this.jsonContent = jsonContent;
    this.maxJsonObjectLen = maxJsonObjectLen;
    this.produceSingleRecord = produceSingleRecord;
    this.topic = topic;
    this.context = context;
  }

  @Override
  public List<Record> createRecords(MessageAndOffset message, int currentRecordCount) throws StageException {
    try (CountingReader reader =
           new CountingReader(new BufferedReader(new InputStreamReader(
             new ByteArrayInputStream(message.getPayload()))))) {
      OverrunStreamingJsonParser parser = new OverrunStreamingJsonParser(reader, jsonContent, maxJsonObjectLen);

      List<Object> objects = new ArrayList<>();
      Object obj = parser.read();
      while(obj != null) {
        objects.add(obj);
        obj = parser.read();
      }

      if(jsonContent == StreamingJsonParser.Mode.ARRAY_OBJECTS) {
        //A single kafka message has an array of json objects
        Record record = RecordCreatorUtil.createRecord(context, topic, message.getPartition(), currentRecordCount++);
        record.set(JsonUtil.jsonToField(objects));
        return ImmutableList.of(record);
      } else {
        //A single kafka message has multiple json objects [not array, multiple]
        //Read all of the json objects
        if(produceSingleRecord) {
          //create one record out of all objects, in which case this record will contain
          // a list of json objects
          Record record = RecordCreatorUtil.createRecord(context, topic, message.getPartition(), currentRecordCount++);
          if(objects.size() == 1) {
            record.set(JsonUtil.jsonToField(objects.get(0)));
            return ImmutableList.of(record);
          } else if (objects.size() == 0) {
            record.set(JsonUtil.jsonToField(obj));
            return ImmutableList.of(record);
          } else {
            record.set(JsonUtil.jsonToField(objects));
            return ImmutableList.of(record);
          }
        } else {
          //create one record per json object, A single kafka message translates to multiple records.
          List<Record> records = new ArrayList<>(objects.size());
          for(Object object : objects) {
            Record record = RecordCreatorUtil.createRecord(context, topic, message.getPartition(),
              currentRecordCount++);
            record.set(JsonUtil.jsonToField(object));
            records.add(record);
          }
          return records;
        }
      }
    } catch (Exception e) {
      throw new StageException(KafkaStageLibError.KFK_0101, e.getMessage(), e);
    }
  }
}
