/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.avro;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.lib.util.SdcAvroTestUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Collections;

public class TestAvroDataFileParser {

  @Test
  public void testAvroDataFileParser() throws Exception {
    File avroDataFile = SdcAvroTestUtil.createAvroDataFile();
    DataParser avroDataFileParser = getDataParser(avroDataFile, 1024, null);

    Record parse = avroDataFileParser.parse();
    Assert.assertNotNull(parse);
    Assert.assertEquals("244::1", avroDataFileParser.getOffset());

    parse = avroDataFileParser.parse();
    Assert.assertNotNull(parse);
    Assert.assertEquals("244::2", avroDataFileParser.getOffset());

    parse = avroDataFileParser.parse();
    Assert.assertNotNull(parse);
    Assert.assertEquals("500::1", avroDataFileParser.getOffset());

    parse = avroDataFileParser.parse();
    Assert.assertNull(parse);
    Assert.assertEquals("-1", avroDataFileParser.getOffset());

  }

  @Test
  public void testAvroDataFileParserOffset() throws Exception {
    File avroDataFile = SdcAvroTestUtil.createAvroDataFile();
    DataParser dataParser = getDataParser(avroDataFile, 1024, null);

    Record parse = dataParser.parse();
    Assert.assertNotNull(parse);
    Assert.assertEquals("244::1", dataParser.getOffset());

    dataParser = getDataParser(avroDataFile, 1024, dataParser.getOffset());
    parse = dataParser.parse();
    Assert.assertNotNull(parse);
    Assert.assertEquals("244::2", dataParser.getOffset());

    dataParser = getDataParser(avroDataFile, 1024, dataParser.getOffset());
    parse = dataParser.parse();
    Assert.assertNotNull(parse);
    Assert.assertEquals("500::1", dataParser.getOffset());

    dataParser = getDataParser(avroDataFile, 1024, dataParser.getOffset());
    parse = dataParser.parse();
    Assert.assertNull(parse);
    Assert.assertEquals("-1", dataParser.getOffset());

  }

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
      Collections.<String>emptyList());
  }

  private DataParser getDataParser(File file, int maxObjectLength, String readerOffset) throws DataParserException {
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(),
      DataParserFormat.AVRO);
    DataParserFactory factory = dataParserFactoryBuilder
      .setMaxDataLen(maxObjectLength)
      .setConfig(AvroDataParserFactory.SCHEMA_KEY, SdcAvroTestUtil.AVRO_SCHEMA)
      .setOverRunLimit(1000)
      .build();
    return factory.getParser(file, readerOffset);
  }
}
