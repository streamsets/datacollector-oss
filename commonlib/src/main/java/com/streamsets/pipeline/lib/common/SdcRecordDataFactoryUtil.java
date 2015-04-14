/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.common;

import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.parser.DataParserException;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class SdcRecordDataFactoryUtil {

  public static final int NUMBER_HEADER_BYTES = 2;
  //10100001 - 101 is the magic number and 1 indicates SDC JSON Record format
  public static final byte SDC_FORMAT_JSON_BYTE = -95;


  public static byte[] readHeader(InputStream inputStream) throws DataParserException {
    byte[] headerBytes = new byte[NUMBER_HEADER_BYTES];
    try {
      IOUtils.read(inputStream, headerBytes);
    } catch (IOException e) {
      throw new DataParserException(com.streamsets.pipeline.lib.parser.sdcrecord.Errors.SDC_RECORD_PARSER_01,
        e.getMessage(), e);
    }
    return headerBytes;
  }

  public static void writeHeader(OutputStream outputStream) throws DataGeneratorException {
    try {
      outputStream.write(
        new byte[] {
          SDC_FORMAT_JSON_BYTE,
          0
        });
    } catch (IOException e) {
      throw new DataGeneratorException(com.streamsets.pipeline.lib.generator.sdcrecord.Errors.SDC_GENERATOR_01,
        e.getMessage(), e);
    }
  }

}
