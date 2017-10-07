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
package com.streamsets.pipeline.lib.xml;

import com.google.common.base.Strings;
import com.streamsets.pipeline.api.ext.io.ObjectLengthException;
import com.streamsets.pipeline.api.ext.io.OverrunException;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;

import static org.junit.Assert.fail;

public class TestOverrunStreamingXmlParser {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    System.getProperties().remove(OverrunReader.READ_LIMIT_SYS_PROP);
  }

  @After
  public void cleanUp() {
    setUp();
  }

  public void testStreamLevelOverrunArray(boolean attemptNextRead) throws Exception {
    System.setProperty(OverrunReader.READ_LIMIT_SYS_PROP, "10000");
    String xml = "<root><record/><record>" + Strings.repeat("a", 20000) + "</record></root>";
    StreamingXmlParser parser = new OverrunStreamingXmlParser(new StringReader(xml), "record", 0, 100);
    Assert.assertNotNull(parser.read());
    if (!attemptNextRead) {
      parser.read();
    } else {
      try {
        parser.read();
      } catch (OverrunException ex) {
        //NOP
      }
      parser.read();
    }
  }

  @Test(expected = OverrunException.class)
  public void testStreamLevelOverrunArray() throws Exception {
    testStreamLevelOverrunArray(false);
  }

  @Test(expected = IllegalStateException.class)
  public void testStreamLevelOverrunArrayAttemptNextRead() throws Exception {
    testStreamLevelOverrunArray(true);
  }

  private Reader getXml(String name) throws Exception {
    return new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(name));
  }

  @Test
  public void testXmlObjectWithLongContentOverrun() throws Exception {
    thrown.expect(ObjectLengthException.class);

    StreamingXmlParser parser = new OverrunStreamingXmlParser(
        getXml("com/streamsets/pipeline/lib/xml/TestOverrunStreamingXmlParser-long-content.xml"),
        null,
        0,
        4096
    );

    parser.read();
    fail("ObjectLengthException should have occurred");
  }

}
