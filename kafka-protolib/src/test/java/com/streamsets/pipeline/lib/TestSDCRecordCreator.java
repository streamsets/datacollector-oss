/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.JsonRecordWriter;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.StringWriter;
import java.util.List;

public class TestSDCRecordCreator {

  @Test
  public void testCreator() throws Exception {
    Source.Context context = ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
                                                                    ImmutableList.of("a"));

    Record record = com.streamsets.pipeline.sdk.RecordCreator.create("s", "id");
    record.set(Field.create("hello"));
    StringWriter writer = new StringWriter();
    JsonRecordWriter rw = ((ContextExtensions) context).createJsonRecordWriter(writer);
    rw.write(record);
    rw.close();
    byte[] payload = writer.toString().getBytes();

    RecordCreator creator = new SDCRecordCreator(context);

    MessageAndOffset message = Mockito.mock(MessageAndOffset.class);
    Mockito.when(message.getPayload()).thenReturn(payload);
    List<Record> list = creator.createRecords(message, 0);

    Assert.assertEquals(1, list.size());
    Assert.assertEquals(record, list.get(0));
  }
}
