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
package com.streamsets.pipeline.stage.destination.solr;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;

public class SolrFieldMappingConfig {

  /**
   * Constructor used for unit testing purposes
   * @param field Field name
   * @param solrFieldName Column name
   */
  public SolrFieldMappingConfig(final String field, final String solrFieldName) {
    this.field = field;
    this.solrFieldName = solrFieldName;
  }

  /**
   * Parameter-less constructor required.
   */
  public SolrFieldMappingConfig() {

  }

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="/",
      label = "Field Path",
      description = "The field-path in the incoming record to output.",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @FieldSelectorModel(singleValued = true)
  public String field;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue="field",
      label = "Solr Field Name",
      description="The Solr field name to write this field to.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String solrFieldName;

  public boolean equals(String fieldName) {
    return this.field.equals(fieldName);
  }
}
