/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.record.RecordImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class TestPreconditionsPredicate {

  @Test
  public void testNullEmptyPreconditions() {
    FilterRecordBatch.Predicate predicate = new PreconditionsPredicate(null);
    Assert.assertTrue(predicate.evaluate(null));
    predicate = new PreconditionsPredicate(Collections.<String>emptyList());
    Assert.assertTrue(predicate.evaluate(null));
  }

  @Test
  public void testOnePrecondition() {
    FilterRecordBatch.Predicate predicate = new PreconditionsPredicate(null);
    Assert.assertTrue(predicate.evaluate(null));
    predicate = new PreconditionsPredicate(Arrays.asList("${record:value('/') == 'Hello'}"));

    Record record = new RecordImpl("", "", null, null);
    record.set(Field.create("Hello"));
    record.set(Field.create("Bye"));
    Assert.assertFalse(predicate.evaluate(record));
    Assert.assertNotNull(predicate.getRejectedMessage());
  }

  @Test
  public void testMultiplePreconditions() {
    FilterRecordBatch.Predicate predicate = new PreconditionsPredicate(null);
    Assert.assertTrue(predicate.evaluate(null));
    predicate = new PreconditionsPredicate(Arrays.asList("${record:value('/') % 2 == 0}",
                                                         "${record:value('/') % 3 == 0}"));

    Record record = new RecordImpl("", "", null, null);
    record.set(Field.create(2));
    Assert.assertFalse(predicate.evaluate(record));
    Assert.assertNotNull(predicate.getRejectedMessage());
    record.set(Field.create(3));
    Assert.assertFalse(predicate.evaluate(record));
    Assert.assertNotNull(predicate.getRejectedMessage());
    record.set(Field.create(6));
    Assert.assertTrue(predicate.evaluate(record));
  }

}
