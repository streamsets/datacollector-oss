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
package com.streamsets.pipeline.lib.parser.wholefile;

import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.lib.parser.AbstractDataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;

import java.io.IOException;
import java.util.Map;

public class WholeFileDataParser extends AbstractDataParser {
  private static final String OFFSET_MINUS_ONE = "-1";
  private static final String OFFSET_ZERO = "0";
  private ProtoConfigurableEntity.Context context;
  private String id;
  private FileRef fileRef;
  private Map<String, Object> metadata;
  private boolean alreadyParsed;
  private boolean isClosed;

  WholeFileDataParser(
      ProtoConfigurableEntity.Context context,
      String id,
      Map<String, Object> metadata,
      FileRef fileRef
  )  {
    this.id = id;
    this.context = context;
    this.metadata = metadata;
    this.fileRef = fileRef;
  }

  @Override
  public Record parse() throws IOException, DataParserException {
    if (isClosed) {
      throw new IOException("The parser is closed");
    }
    if (!alreadyParsed) {
      Record record = context.createRecord(id);
      record.set(FileRefUtil.getWholeFileRecordRootField(fileRef, metadata));
      alreadyParsed = true;
      return record;
    }
    return null;
  }

  @Override
  public String getOffset() throws DataParserException, IOException {
    //Will return the offset -1 if already parsed else return 0.
    return (!alreadyParsed)? OFFSET_ZERO : OFFSET_MINUS_ONE;
  }

  @Override
  public void close() throws IOException {
    isClosed = true;
  }


}
