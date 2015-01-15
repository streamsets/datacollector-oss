/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.csv;

import com.google.common.base.Strings;
import com.streamsets.pipeline.lib.io.OverrunException;
import com.streamsets.pipeline.lib.io.OverrunReader;
import org.apache.commons.csv.CSVFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.StringReader;

public class TestOverrunCsvParser {

  @Before
  public void setUp() {
    System.getProperties().remove(OverrunReader.READ_LIMIT_SYS_PROP);
  }

  @After
  public void cleanUp() {
    setUp();
  }

  private void testLimit(int limit, int lineLength) throws Exception {
    System.setProperty(OverrunReader.READ_LIMIT_SYS_PROP, "" + limit);
    String csv = "a," + Strings.repeat("b", lineLength) + ",c";
    OverrunCsvParser parser = new OverrunCsvParser(new StringReader(csv), CSVFormat.DEFAULT);
    Assert.assertNotNull(parser.read());
  }

  @Test
  public void testUnderLimit() throws Exception {
    testLimit(10000, 8000);
  }

  @Test(expected = OverrunException.class)
  public void testOverLimit() throws Exception {
    testLimit(10000, 11000);
  }

}
