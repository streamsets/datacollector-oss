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

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.CreateByRef;
import com.streamsets.pipeline.lib.util.ExceptionUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Map;

public class WrapperDataParserFactory extends DataParserFactory {
  private final DataParserFactory factory;

  public WrapperDataParserFactory(DataParserFactory factory) {
    super(factory.getSettings());
    this.factory = factory;
  }

  @Override
  public DataParser getParser(String id, byte[] data, int offset, int len) throws DataParserException {
    return new WrapperDataParser(factory.getParser(id, data, offset, len));
  }

  @Override
  public DataParser getParser(String id, byte[] data) throws DataParserException {
    return new WrapperDataParser(factory.getParser(id, data));
  }

  @Override
  public DataParser getParser(String id, String data) throws DataParserException {
    return new WrapperDataParser(factory.getParser(id, data));
  }

  @Override
  public DataParser getParser(String id, Reader reader) throws DataParserException {
    return new WrapperDataParser(factory.getParser(id, reader));
  }

  @Override
  public DataParser getParser(File file, String fileOffset) throws DataParserException {
    return new WrapperDataParser(factory.getParser(file, fileOffset));
  }

  @Override
  public DataParser getParser(String id, InputStream is, String offset) throws DataParserException {
    return new WrapperDataParser(factory.getParser(id, is, offset));
  }

  @Override
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    return new WrapperDataParser(factory.getParser(id, reader, offset));
  }

  @Override
  public DataParser getParser(
      String id,
      Map<String, Object> metadata,
      FileRef fileRef
  ) throws DataParserException {
    return new WrapperDataParser(factory.getParser(id, metadata, fileRef));
  }

  @Override
  public void destroy() {
    factory.destroy();
    super.destroy();
  }

  @VisibleForTesting
  public DataParserFactory getFactory() {
    return factory;
  }

  private static class WrapperDataParser implements DataParser {
    private final DataParser dataParser;


    public WrapperDataParser(DataParser dataParser) {
      this.dataParser = dataParser;
    }

    @Override
    public Record parse() throws IOException, DataParserException {
      try {
        return CreateByRef.call( () -> dataParser.parse());
      } catch (Exception ex) {
        ExceptionUtils.throwUndeclared(normalizeException(ex));
      }
      return null; //unreacheable
    }

    @Override
    public String getOffset() throws DataParserException, IOException {
      try {
        return dataParser.getOffset();
      } catch (Exception ex) {
        ExceptionUtils.throwUndeclared(normalizeException(ex));
      }
      return null; //unreacheable
    }

    @Override
    public void setTruncated() {
      dataParser.setTruncated();
    }

    @Override
    public void close() throws IOException {
      try {
        dataParser.close();
      } catch (Exception ex) {
        ExceptionUtils.throwUndeclared(normalizeException(ex));
      }
    }

    Throwable normalizeException(Throwable ex) {
      if (!(ex instanceof IOException) && !(ex instanceof DataParserException)) {
        if (ex.getCause() != null) {
          ex = ex.getCause();
          if (!(ex instanceof IOException) && !(ex instanceof DataParserException)) {
            if (ex instanceof StageException) {
              StageException seCause = (StageException) ex;
              ex = new DataParserException(seCause.getErrorCode(), seCause.getParams());
            }
          }
        }
        ex = new DataParserException(Errors.DATA_PARSER_02, ex.toString(), ex);
      }
      return ex;
    }

  }

}
