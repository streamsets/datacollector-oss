/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.io.OverrunReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.util.UUID;

public class TestCharDataParserFactory {

  private static final DataParser DATA_PARSER = new DataParser() {
    @Override
    public Record parse() throws IOException, DataParserException {
      return null;
    }

    @Override
    public long getOffset() throws DataParserException {
      return 0;
    }

    @Override
    public void close() throws IOException {

    }
  };

  private static class TestFactory extends CharDataParserFactory {
    private final String id;
    private final OverrunReader reader;
    private final long readerOffset;
    private final boolean assertReader;

    public TestFactory(String id, OverrunReader reader, long readerOffset, boolean assertReader) {
      this.id = id;
      this.reader = reader;
      this.readerOffset = readerOffset;
      this.assertReader = assertReader;
    }

    @Override
    public DataParser getParser(String id, OverrunReader reader, long readerOffset) throws DataParserException {
      Assert.assertEquals(this.id, id);
      if (assertReader) {
        Assert.assertEquals(this.reader, reader);
      }
      Assert.assertEquals(this.readerOffset, readerOffset);
      return DATA_PARSER;
    }
  }

  @Test
  public void testFactoryMethods() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(""), 1, true);
    CharDataParserFactory factory = new TestFactory("id", reader, 10, true);
    Assert.assertEquals(DATA_PARSER, factory.getParser("id", reader, 10));

    factory = new TestFactory("id", reader, 0, false);
    Assert.assertEquals(DATA_PARSER, factory.getParser("id", ""));

    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    File file = new File(dir, "abc");
    Files.createFile(file.toPath());
    factory = new TestFactory(file.getName(), reader, 10, false);
    Assert.assertEquals(DATA_PARSER, factory.getParser(file, 10, 10));
  }

  @Test
  public void testBuilder() throws Exception {

  }
}
