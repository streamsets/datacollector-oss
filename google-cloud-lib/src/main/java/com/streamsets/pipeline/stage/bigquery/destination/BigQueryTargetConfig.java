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
package com.streamsets.pipeline.stage.bigquery.destination;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.lib.GoogleCloudCredentialsConfig;

import java.util.List;

public class BigQueryTargetConfig {

  @ConfigDef(
      required = true,
      label = "Dataset Expression",
      type = ConfigDef.Type.STRING,
      defaultValue = "${record:attribute('dataset')}",
      description = "Use an expression language to obtain dataset name from record",
      displayPosition = 10,
      group = "BIGQUERY",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class}
  )
  public String datasetEL;

  @ConfigDef(
      required = true,
      label = "Table Name Expression",
      type = ConfigDef.Type.STRING,
      defaultValue = "${record:attribute('table')}",
      description = "Use an expression language to obtain table name name from record",
      displayPosition = 20,
      group = "BIGQUERY",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class}
  )
  public String tableNameEL;

  @ConfigDef(
      //Not needed, if not configured, its considered a plain insert without row id
      required = false,
      label = "Row Id Expression",
      type = ConfigDef.Type.STRING,
      description = "Use an expression language to specify a row id to perform insert/update",
      displayPosition = 30,
      group = "BIGQUERY",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class}
  )
  public String rowIdExpression;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Ignore Invalid Column",
      description = "If enabled, field paths that cannot be mapped to column will be ignored",
      displayPosition = 40,
      group = "BIGQUERY"
  )
  public boolean ignoreInvalidColumn;

  @ConfigDef(
      required =  true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "-1",
      label = "Table cache",
      description = "Configures the cache size for storing TableId entries." +
          " Use -1 for unlimited number of tableId entries in the cache.",
      displayPosition = 50,
      group = "BIGQUERY",
      min = -1,
      max = Integer.MAX_VALUE
  )
  public int maxCacheSize = -1;

  @ConfigDefBean(groups = "CREDENTIALS")
  public GoogleCloudCredentialsConfig credentials = new GoogleCloudCredentialsConfig();

}
