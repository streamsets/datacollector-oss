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
package com.streamsets.pipeline.lib.io.fileref;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.generator.StreamCloseEventHandler;

import java.util.Map;

public class FileRefStreamCloseEventHandler implements StreamCloseEventHandler<Map<String, Object>> {
  private final Record eventRecord;

  public FileRefStreamCloseEventHandler(Record eventRecord) {
    this.eventRecord = eventRecord;
  }

  @Override
  public void handleCloseEvent(Map<String, Object> eventInfo) {
    if (eventInfo.containsKey(FileRefUtil.WHOLE_FILE_CHECKSUM_ALGO)) {
      Utils.checkState(
          eventInfo.containsKey(FileRefUtil.WHOLE_FILE_CHECKSUM),
          "Calculated checksum should be present in the info"
      );
      Map<String, Field> rootField = eventRecord.get().getValueAsMap();
      rootField.put(
          FileRefUtil.WHOLE_FILE_CHECKSUM_ALGO,
          Field.create(Field.Type.STRING, eventInfo.get(FileRefUtil.WHOLE_FILE_CHECKSUM_ALGO))
      );
      rootField.put(
          FileRefUtil.WHOLE_FILE_CHECKSUM,
          Field.create(Field.Type.STRING, eventInfo.get(FileRefUtil.WHOLE_FILE_CHECKSUM))
      );
    }
  }
}
