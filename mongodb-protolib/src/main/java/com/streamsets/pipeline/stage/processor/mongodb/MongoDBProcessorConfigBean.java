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


import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.stage.common.MissingValuesBehavior;
import com.streamsets.pipeline.stage.common.MissingValuesBehaviorChooserValues;
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import com.streamsets.pipeline.stage.common.mongodb.MongoDBConfig;
import com.streamsets.pipeline.stage.origin.mongodb.ReadPreferenceChooserValues;
import com.streamsets.pipeline.stage.origin.mongodb.ReadPreferenceLabel;
import com.streamsets.pipeline.stage.processor.kv.CacheConfig;
import java.util.List;

public class MongoDBProcessorConfigBean {

  @ConfigDefBean(groups = {"MONGODB", "CREDENTIALS", "ADVANCED"})
  public MongoDBConfig mongoConfig;

  @ConfigDef(
      type = ConfigDef.Type.MODEL,
      label = "Document to SDC Field Mappings",
      description = "Mapping between SDC record to Document fields to construct a find() query",
      required = true,
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "LOOKUP"
  )
  @ListBeanModel
  public List<MongoDBFieldColumnMapping> fieldMapping;

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      label = "Result Field",
      description = "Field name to store lookup result. It should start with '/'",
      required = true,
      displayPosition = 55,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "LOOKUP"
  )
  @ListBeanModel
  public String resultField;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Multiple Values Behavior",
      description = "How to handle multiple values",
      defaultValue = "FIRST_ONLY",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "LOOKUP"
  )
  @ValueChooserModel(MongoDBLookupMultipleValuesBehaviorChooserValues.class)
  public MultipleValuesBehavior multipleValuesBehavior = MultipleValuesBehavior.DEFAULT;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Missing Values Behavior",
      description = "How to handle missing values",
      defaultValue = "PASS_RECORD_ON",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "LOOKUP"
  )
  @ValueChooserModel(MissingValuesBehaviorChooserValues.class)
  public MissingValuesBehavior missingValuesBehavior = MissingValuesBehavior.DEFAULT;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "SECONDARY_PREFERRED",
      label = "Read Preference",
      description = "Sets the read preference",
      group = "MONGODB",
      displayPosition = 180,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ValueChooserModel(ReadPreferenceChooserValues.class)
  public ReadPreferenceLabel readPreference;

  @ConfigDefBean(groups = "LOOKUP")
  public CacheConfig cacheConfig = new CacheConfig();
}
