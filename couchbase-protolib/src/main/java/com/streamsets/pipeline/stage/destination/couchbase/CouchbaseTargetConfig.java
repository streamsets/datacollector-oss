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
package com.streamsets.pipeline.stage.destination.couchbase;

import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.pipeline.lib.couchbase.BaseCouchbaseConfig;

public class CouchbaseTargetConfig extends BaseCouchbaseConfig{

  /**
   * Document tab
   */
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "${record:attribute('doc_id')}",
      label = "Document Key",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      description = "Document key to write",
      group = "DOCUMENT",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String documentKeyEL;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "0",
      label = "Document Time-To-Live (seconds)",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      description = "Expiry TTL to apply to each document. 0 or blank means no expiry",
      group = "DOCUMENT",
      elDefs = {RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String documentTtlEL;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UPSERT",
      label = "Default Write Operation",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      description = "Write operation to perform if CDC sdc.operation.type attribute is not set " +
          "or if sub-document operation type is not specified",
      group = "DOCUMENT"
  )
  @ValueChooserModel(WriteOperationChooserValues.class)
  public WriteOperationType defaultWriteOperation = WriteOperationType.UPSERT;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "DEFAULT",
      label = "Unsupported Operation Handling",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      description = "Action to take when CDC or sub-document operation type is not supported",
      group = "DOCUMENT"
  )
  @ValueChooserModel(UnsupportedOperationChooserValues.class)
  public UnsupportedOperationType unsupportedOperation = UnsupportedOperationType.DEFAULT;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Use CAS",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      description = "Use the couchbase.cas record attribute for writes, if present",
      group = "DOCUMENT"
  )
  public boolean useCas;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Allow Sub-Document Writes",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      description = "Allow sub-document writes",
      group = "DOCUMENT"
  )
  public boolean allowSubdoc;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "${record:attribute('subdoc_path')}",
      label = "Sub-Document Path",
      displayPosition = 61,
      displayMode = ConfigDef.DisplayMode.BASIC,
      description = "The path at which to store the sub-document. If no path is provided, " +
          "a full document write will be performed",
      group = "DOCUMENT",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      dependsOn = "allowSubdoc",
      triggeredByValue = "true"
  )
  public String subdocPathEL;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "${record:attribute('subdoc_operation')}",
      label = "Sub-Document Operation",
      displayPosition = 62,
      displayMode = ConfigDef.DisplayMode.BASIC,
      description = "The sub-document write operation to use. " +
          "Supported operations are: INSERT, REPLACE, UPSERT, DELETE, ARRAY_PREPEND, ARRAY_APPEND, ARRAY_ADD_UNIQUE. " +
          "If not specified, the CDC sdc.operation.type attribute or Default Write Operation will be used",
      group = "DOCUMENT",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      dependsOn = "allowSubdoc",
      triggeredByValue = "true"
  )
  public String subdocOperationEL;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NONE",
      label = "Replicate To",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC,
      description = "Number of copies of a document to replicate before returning success. " +
          "This must be less than or equal to the number of replicas configured on your bucket",
      group = "DOCUMENT"
  )
  @ValueChooserModel(ReplicateToChooserValues.class)
  public ReplicateTo replicateTo = ReplicateTo.NONE;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NONE",
      label = "Persist To",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.BASIC,
      description = "Number of copies of a document to persist before returning success. " +
          "This must be less than or equal to 1 + the number of replicas configured on your bucket",
      group = "DOCUMENT"
  )
  @ValueChooserModel(PersistToChooserValues.class)
  public PersistTo persistTo = PersistTo.NONE;


  /**
   * Data Format Tab
   */
  @ConfigDefBean(groups = {"DATA_FORMAT"})
  public DataGeneratorFormatConfig dataFormatConfig;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JSON",
      label = "Data Format",
      description = "Data format to use when writing records to Couchbase",
      displayPosition = 1,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;
}
