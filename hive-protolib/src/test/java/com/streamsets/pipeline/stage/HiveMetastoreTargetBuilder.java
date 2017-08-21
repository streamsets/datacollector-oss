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
package com.streamsets.pipeline.stage;

import com.streamsets.pipeline.stage.destination.hive.HMSTargetConfigBean;
import com.streamsets.pipeline.stage.destination.hive.HiveMetastoreTarget;

public class HiveMetastoreTargetBuilder {

  private boolean storedAsAvro;
  private String hdfsUser;
  private String schemaFolderLocation = ".schemas";

  public HiveMetastoreTargetBuilder() {
    this.storedAsAvro = true;
  }

  public HiveMetastoreTargetBuilder storedAsAvro(boolean storedAsAvro) {
    this.storedAsAvro = storedAsAvro;
    return this;
  }

  public HiveMetastoreTargetBuilder schemFolderLocation(String schemaFolderLocation) {
    this.schemaFolderLocation = schemaFolderLocation;
    return this;
  }

  public HiveMetastoreTargetBuilder hdfsUser(String user) {
    this.hdfsUser = user;
    return this;
  }

  public HiveMetastoreTarget build() {
    HMSTargetConfigBean config = new HMSTargetConfigBean();
    config.storedAsAvro = storedAsAvro;
    config.hdfsUser = hdfsUser;
    config.hiveConfigBean = BaseHiveIT.getHiveConfigBean();
    config.schemaFolderLocation = schemaFolderLocation;
    return new HiveMetastoreTarget(config);
  }

}
