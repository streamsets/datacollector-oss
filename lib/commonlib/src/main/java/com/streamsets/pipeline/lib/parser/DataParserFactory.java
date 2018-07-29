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
package com.streamsets.pipeline.lib.parser;

import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;

public abstract class DataParserFactory extends DataFactory {

  private static final int DEFAULT_MAX_RECORD_LENGTH = 1024;

  protected DataParserFactory(Settings settings) {
    super(settings);
    Utils.checkState(settings.getMaxRecordLen() != 0, "maxDataLen has not been set");
  }

  public DataParser getParser(String id, byte[] data, int offset, int len) throws DataParserException {
    return getParser(id, new ByteArrayInputStream(data, offset, len), "0");
  }

  public DataParser getParser(String id, byte[] data) throws DataParserException {
    return getParser(id, data, 0, data.length);
  }

  public DataParser getParser(String id, String data) throws DataParserException {
    return getParser(id, new StringReader(data));
  }

  public DataParser getParser(String id, Reader reader) throws DataParserException {
    return getParser(id, reader, 0);
  }

  public DataParser getParser(File file, String fileOffset)
    throws DataParserException {
    try {
      return getParser(file.getName(), new FileInputStream(file), fileOffset);
    } catch (FileNotFoundException e) {
      throw new DataParserException(Errors.DATA_PARSER_00, file.getAbsolutePath(), e.toString(), e);
    }
  }

  public abstract DataParser getParser(String id, InputStream is, String offset) throws DataParserException;

  public abstract DataParser getParser(String id, Reader reader, long offset) throws DataParserException;

  public DataParser getParser(
      String id,
      Map<String, Object> metadata,
      FileRef fileRef
  ) throws DataParserException {
    throw new UnsupportedOperationException();
  }

  protected OverrunReader createReader(InputStream is) {
    Reader bufferedReader = new BufferedReader(new InputStreamReader(is, getSettings().getCharset()));
    return new OverrunReader(bufferedReader,
        getSettings().getOverRunLimit(),
        false,
        getSettings().getRemoveCtrlChars()
    );
  }

  protected OverrunReader createReader(Reader reader) {
    return new OverrunReader(
        bufferReader(reader),
        getSettings().getOverRunLimit(),
        false,
        getSettings().getRemoveCtrlChars()
    );
  }

  private Reader bufferReader(Reader reader) {
    if (reader instanceof BufferedReader) {
      return reader;
    }
    return new BufferedReader(reader);
  }

  protected GenericObjectPool<StringBuilder> getStringBuilderPool(Settings settings) {
    int maxRecordLen = getSettings().getMaxRecordLen();
    int poolSize = getSettings().getStringBuilderPoolSize();
    GenericObjectPoolConfig stringBuilderPoolConfig = new GenericObjectPoolConfig();
    stringBuilderPoolConfig.setMaxTotal(poolSize);
    stringBuilderPoolConfig.setMinIdle(poolSize);
    stringBuilderPoolConfig.setMaxIdle(poolSize);
    stringBuilderPoolConfig.setBlockWhenExhausted(false);
    return new GenericObjectPool<>(
      new StringBuilderPoolFactory(maxRecordLen > 0 ? maxRecordLen : DEFAULT_MAX_RECORD_LENGTH),
      stringBuilderPoolConfig
    );
  }

}
