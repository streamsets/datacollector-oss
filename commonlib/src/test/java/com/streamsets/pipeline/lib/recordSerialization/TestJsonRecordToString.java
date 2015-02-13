/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.recordSerialization;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.JsonRecordReader;
import com.streamsets.pipeline.lib.recordserialization.JsonRecordToString;
import com.streamsets.pipeline.lib.recordserialization.RecordToString;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

public class TestJsonRecordToString {

  @Test
  public void testRecordToString() throws IOException, StageException {
    Target.Context context = ContextInfoCreator.createTargetContext("t", false);
    RecordToString recordToString = new JsonRecordToString(context);
    Record record = RecordCreator.create();
    record.set(Field.create("hello"));
    String str = recordToString.toString(record);

    JsonRecordReader rr = ((ContextExtensions)context).createJsonRecordReader(new StringReader(str), 0, Integer.MAX_VALUE);
    Record newRecord = rr.readRecord();
    rr.close();
    Assert.assertEquals(record, newRecord);
  }

}
