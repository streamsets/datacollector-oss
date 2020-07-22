/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.sdk.service;

import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseService;
import com.streamsets.pipeline.api.service.ServiceDef;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.api.service.dataformats.DataParser;
import com.streamsets.pipeline.api.service.dataformats.DataParserException;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Map;

@ServiceDef(
  provides = DataFormatParserService.class,
  version = 1,
  label = "(Test) Runner implementation of very simple DataFormatParserService that will always parse input as Whole File Format."
)
public class SdkWholeFileDataFormatParserService extends BaseService implements DataFormatParserService {
  @Override
  public DataParser getParser(String id, InputStream is, String offset) throws DataParserException {
    throw new UnsupportedOperationException("Only Whole File Format is supported");
  }

  @Override
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    throw new UnsupportedOperationException("Only Whole File Format is supported");
  }

  @Override
  public DataParser getParser(String id, Map<String, Object> metadata, FileRef fileRef) throws DataParserException {
    return new DataParserImpl(metadata, fileRef);
  }

  @Override
  public String getCharset() {
    return null;
  }

  @Override
  @Deprecated
  public void setStringBuilderPoolSize(int poolSize) {
    // N0-op
  }

  @Override
  @Deprecated
  public int getStringBuilderPoolSize() {
    return 0;
  }

  @Override
  public boolean isWholeFileFormat() {
    return true;
  }

  @Override
  public long suggestedWholeFileBufferSize() {
    return 1024;
  }

  @Override
  public Double wholeFileRateLimit() throws StageException {
    return -1d;
  }

  @Override
  public boolean isWholeFileChecksumRequired() {
    return true;
  }

  private static class DataParserImpl implements DataParser {

    private final Map<String, Object> metadata;
    private final FileRef fileRef;
    private boolean parsed;

    DataParserImpl(Map<String, Object> metadata, FileRef fileRef) {
      this.metadata = metadata;
      this.fileRef = fileRef;
      this.parsed = false;
    }

    @Override
    public Record parse() throws IOException, DataParserException {
      if(parsed) {
        return null;
      }

      parsed = true;

      Record record = RecordCreator.create();
      record.set(FileRefUtil.getWholeFileRecordRootField(fileRef, metadata));
      return record;
    }

    @Override
    public String getOffset() throws DataParserException, IOException {
      return "-1";
    }

    @Override
    public void setTruncated() {
    }

    @Override
    public void close() throws IOException {
    }
  }
}
