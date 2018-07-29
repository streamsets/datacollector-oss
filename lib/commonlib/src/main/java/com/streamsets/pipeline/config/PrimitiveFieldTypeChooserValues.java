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
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public class PrimitiveFieldTypeChooserValues extends BaseEnumChooserValues<Field.Type> {

  public PrimitiveFieldTypeChooserValues() {
    super(Field.Type.BOOLEAN, Field.Type.CHAR, Field.Type.BYTE, Field.Type.SHORT, Field.Type.INTEGER, Field.Type.LONG,
          Field.Type.FLOAT, Field.Type.DOUBLE, Field.Type.DECIMAL, Field.Type.DATE, Field.Type.TIME, Field.Type.DATETIME,
          Field.Type.ZONED_DATETIME, Field.Type.STRING, Field.Type.BYTE_ARRAY);
  }

}
