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
package com.external.stage;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;

@StageDef(name = "FacebookSource", description = "Produces facebook feeds", label = "facebook_source"
, version = "1.0")
public class FacebookSource extends BaseSource{

  @ConfigDef(
    name = "username",
    defaultValue = "admin",
    label = "username",
    required = true,
    description = "The user name of the facebook user",
    type = ConfigDef.Type.STRING
  )
  public String username;

  @ConfigDef(
    name = "password",
    defaultValue = "admin",
    label = "password",
    required = true,
    description = "The password the facebook user",
    type = ConfigDef.Type.STRING
  )
  public String password;

  public FacebookSource() {

  }
  public FacebookSource(String username, String password) {
    this.username = username;
    this.password = password;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  @Override
  public String produce(String lastSourceOffset, BatchMaker batchMaker) throws StageException {
    return null;
  }
}
