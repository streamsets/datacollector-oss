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
package com.streamsets.pipeline.lib.csv;

import com.google.common.base.Strings;
import com.streamsets.pipeline.api.ext.io.OverrunException;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
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
    OverrunReader.reInitializeDefaultReadLimit();
  }

  @After
  public void cleanUp() {
    setUp();
  }

  private void testLimit(int limit, int lineLength) throws Exception {
    System.setProperty(OverrunReader.READ_LIMIT_SYS_PROP, "" + limit);
    OverrunReader.reInitializeDefaultReadLimit();
    String csv = "a," + Strings.repeat("b", lineLength) + ",c";
    OverrunCsvParser parser = new OverrunCsvParser(new StringReader(csv), CSVFormat.DEFAULT, -1);
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
