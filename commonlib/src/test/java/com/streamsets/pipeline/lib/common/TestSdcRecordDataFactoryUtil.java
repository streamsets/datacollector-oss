/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.common;

import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.parser.DataParserException;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class TestSdcRecordDataFactoryUtil {

  @Test
  public void testWriteDataFormatNumber() throws DataGeneratorException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    SdcRecordDataFactoryUtil.writeHeader(baos);
    byte[] bytes = baos.toByteArray();
    Assert.assertEquals(SdcRecordDataFactoryUtil.NUMBER_HEADER_BYTES, bytes.length);
    Assert.assertEquals(SdcRecordDataFactoryUtil.SDC_FORMAT_JSON_BYTE, bytes[0]);
    Assert.assertEquals(0, bytes[1]);
  }

  @Test
  public void testReadDataFormatNumber() throws DataGeneratorException, DataParserException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    SdcRecordDataFactoryUtil.writeHeader(baos);

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    byte[] bytes = SdcRecordDataFactoryUtil.readHeader(bais);
    Assert.assertEquals(SdcRecordDataFactoryUtil.NUMBER_HEADER_BYTES, bytes.length);
    Assert.assertEquals(SdcRecordDataFactoryUtil.SDC_FORMAT_JSON_BYTE, bytes[0]);
    Assert.assertEquals(0, bytes[1]);
  }

}
