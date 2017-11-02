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
package com.streamsets.pipeline.stage.destination.kudu;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.operation.ChangeLogFormat;
import com.streamsets.pipeline.lib.operation.ChangeLogFormatChooserValues;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationActionChooserValues;
import com.streamsets.pipeline.stage.lib.kudu.KuduFieldMappingConfig;

import java.util.List;

public class KuduConfigBean {

  public static final String CONF_PREFIX = "kuduConfigBean.";

  // kudu tab
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Kudu Masters",
      description = "Comma-separated list of \"host:port\" pairs of the masters",
      displayPosition = 10,
      group = "KUDU"
  )
  public String kuduMaster;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      defaultValue = "${record:attribute('tableName')}",
      label = "Table Name",
      description = "Kudu table to write to. If table doesn't exist, records will be treated as error records.",
      displayPosition = 20,
      group = "KUDU"
  )
  public String tableNameTemplate;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Field to Column Mapping",
      description = "Optionally specify additional field mappings when input field name and column name don't match.",
      displayPosition = 30,
      group = "KUDU"
  )
  @ListBeanModel
  public List<KuduFieldMappingConfig> fieldMappingConfigs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Default Operation",
      description = "Default operation to perform if sdc.operation.type is not set in record header.",
      displayPosition = 40,
      group = "KUDU"
  )
  @ValueChooserModel(KuduOperationChooserValues.class)
  public KuduOperationType defaultOperation;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Change Log Format",
      defaultValue = "NONE",
      description = "If input is a change data capture log, specify the format.",
      displayPosition = 40,
      group = "KUDU"
  )
  @ValueChooserModel(ChangeLogFormatChooserValues.class)
  public ChangeLogFormat changeLogFormat = ChangeLogFormat.NONE;

  // advanced tab
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "CLIENT_PROPAGATED",
      label = "External Consistency",
      description = "The external consistency mode",
      displayPosition = 10,
      group = "ADVANCED"
  )
  @ValueChooserModel(ConsistencyModeChooserValues.class)
  public ConsistencyMode consistencyMode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Mutation Buffer Space (records)",
      description = "Sets the buffer size that Kudu client uses for a single batch. Should be greater than or" +
        " equal to the number of records in the batch passed from the pipeline.",
      defaultValue = "1000",
      displayPosition = 15,
      group = "ADVANCED"
  )
  public int mutationBufferSpace;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "10000",
      label = "Operation Timeout Milliseconds",
      description = "Sets the default timeout used for user operations (using sessions and scanners)",
      displayPosition = 20,
      group = "ADVANCED"
  )
  public int operationTimeout;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue= "DISCARD",
      label = "Unsupported Operation Handling",
      description = "Action to take when operation type is not supported",
      displayPosition = 30,
      group = "ADVANCED"
  )
  @ValueChooserModel(UnsupportedOperationActionChooserValues.class)
  public UnsupportedOperationAction unsupportedAction;
}
