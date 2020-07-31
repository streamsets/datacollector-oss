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
package com.streamsets.pipeline.stage.processor.mongodb;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;

public class MongoDBFieldColumnMapping {
  
  public MongoDBFieldColumnMapping() {}

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue="",
      label = "Document Field",
      description = "Field name in document. It allows dot notation for embedded documents. " +
              "For example 'field1.field2.field3'",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String keyName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "SDC Field",
      description = "The field in the record to receive the value.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @FieldSelectorModel(singleValued = true)
  public String sdcField;

  /**
   * Constructor used for unit testing purposes
   * @param keyName
   * @param field
   */
  public MongoDBFieldColumnMapping(final String keyName, final String field) {
    this.keyName = keyName;
    this.sdcField = field;
  }
}
