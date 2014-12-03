/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.stage.devtest;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;

@GenerateResourceBundle
@StageDef(version="1.0.0", label="Hbase Target", icon = "HbaseTarget.svg")
public class HbaseTarget extends BaseTarget {

  @ConfigDef(required = true, type = ConfigDef.Type.STRING, label = "Hbase URI", defaultValue = "",
            description = "Hbase server URI")
  public String uri;

  @ConfigDef(required = true, type = ConfigDef.Type.BOOLEAN, label = "Security enabled",
             defaultValue = "false", description = "Kerberos enabled for Hbase")
  public boolean security;

  @ConfigDef(required = true, type = ConfigDef.Type.STRING, label = "Table", defaultValue = "",
             description = "Hbase table name")
  public String table;

  @Override
  public void write(Batch batch) throws StageException {
  }

}
