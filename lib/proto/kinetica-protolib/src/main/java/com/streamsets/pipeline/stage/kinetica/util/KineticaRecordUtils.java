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
package com.streamsets.pipeline.stage.kinetica.util;

import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;

public class KineticaRecordUtils {

  public static com.gpudb.Record createKineticaRecordFromStreamsetsRecord(Record streamsetsRecord, Type type) {

    // Create a Kinetica "record" (and avoid name collisions with StreamSets
    // "record")
    com.gpudb.Record kineticaRecord = type.newInstance();

    // Set the value for each column in the Kinetica record
    for (int index = 0; index < type.getColumnCount(); index++) {

      // Get a column from the Kinetica table type
      Column column = type.getColumn(index);

      // Get the Kinetica column name
      String columnName = column.getName();

      // Append "/" to get the StreamSets field name
      String streamSetsFieldName = "/" + columnName;

      Field field = streamsetsRecord.get(streamSetsFieldName);
      if (field != null) {

        // Get the value from the StreamSets record
        Object value = field.getValue();

        // Set the value in the Kinetica record
        kineticaRecord.put(index, value);
      }
    }
    return kineticaRecord;
  }

}
