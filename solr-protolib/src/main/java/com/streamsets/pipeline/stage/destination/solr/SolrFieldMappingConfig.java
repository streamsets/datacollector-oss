/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.solr;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelector;

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
      label = "Field-path",
      description = "The field-path in the incoming record to output.",
      displayPosition = 10
  )
  @FieldSelector(singleValued = true)
  public String field;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue="field",
      label = "Solr Field Name",
      description="The Solr field name to write this field to.",
      displayPosition = 20
  )
  public String solrFieldName;
}
