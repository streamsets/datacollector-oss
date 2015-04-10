/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser;

import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.io.OverrunReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;

public abstract class CharDataParserFactory extends DataFactory {

  protected CharDataParserFactory(Settings settings) {
    super(settings);
  }

  public DataParser getParser(String id, String data) throws DataParserException {
    return getParser(id, new OverrunReader(new StringReader(data), 0, false), 0);
  }

  public DataParser getParser(File file, Charset charset, int overrunLimit, long fileOffset) throws DataParserException {
    Reader fileReader;
    try {
      InputStream fis = new FileInputStream(file);
      fileReader = new InputStreamReader(fis, charset);
      fileReader = new BufferedReader(fileReader);
    } catch (IOException ex) {
      throw new DataParserException(Errors.DATA_PARSER_00, file.getAbsolutePath(), ex.getMessage(), ex);
    }
    try {
      OverrunReader reader = new OverrunReader(fileReader, overrunLimit, false);
      return getParser(file.getName(), reader, fileOffset);
    } catch (DataParserException ex) {
      try {
        fileReader.close();
      } catch (IOException ioEx) {
        //NOP
      }
      throw ex;
    }
  }

  // the reader must be in position zero, the getParser() call will fast forward while initializing the parser
  public abstract DataParser getParser(String id, OverrunReader reader, long readerOffset) throws DataParserException;

}
