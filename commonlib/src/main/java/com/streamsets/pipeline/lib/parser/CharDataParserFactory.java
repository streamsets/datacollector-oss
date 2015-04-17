/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.io.OverrunReader;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

public abstract class CharDataParserFactory extends DataFactory {

  protected CharDataParserFactory(Settings settings) {
    super(settings);
    Utils.checkState(settings.getMaxRecordLen() != 0, "maxDataLen has not been set");
  }

  public DataParser getParser(String id, byte[] data, int offset, int len) throws DataParserException {
    return getParser(id, new ByteArrayInputStream(data, offset, len), 0);
  }

  public DataParser getParser(String id, byte[] data) throws DataParserException {
    return getParser(id, data, 0, data.length);
  }

  public DataParser getParser(File file, long fileOffset)
    throws DataParserException {
    try {
      return getParser(file.getName(), new FileInputStream(file), fileOffset);
    } catch (FileNotFoundException e) {
      throw new DataParserException(Errors.DATA_PARSER_00, file.getAbsolutePath(), e.getMessage(), e);
    }
  }

  public abstract DataParser getParser(String id, InputStream is, long offset) throws DataParserException;

  protected OverrunReader createReader(InputStream is) {
    Reader bufferedReader = new BufferedReader(new InputStreamReader(is, getSettings().getCharset()));
    OverrunReader overrunReader = new OverrunReader(bufferedReader, getSettings().getOverRunLimit(), false);
    return overrunReader;
  }

}
