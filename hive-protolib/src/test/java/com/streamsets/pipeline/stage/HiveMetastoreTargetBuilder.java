/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

  private boolean useAsAvro;
  private String hdfsUser;
  private boolean hdfsKerberos;

  public HiveMetastoreTargetBuilder() {
    this.useAsAvro = true;
  }

  public HiveMetastoreTargetBuilder useAsAvro(boolean useAsAvro) {
    this.useAsAvro = useAsAvro;
    return this;
  }

  public HiveMetastoreTargetBuilder hdfsUser(String user) {
    this.hdfsUser = user;
    return this;
  }

  public HiveMetastoreTargetBuilder hdfsKerberos(boolean kerberos) {
    this.hdfsKerberos = kerberos;
    return this;
  }

  public HiveMetastoreTarget build() {
    HMSTargetConfigBean config = new HMSTargetConfigBean();
    config.useAsAvro = useAsAvro;
    config.hdfsUser = hdfsUser;
    config.hdfsKerberos = hdfsKerberos;
    config.hiveConfigBean = BaseHiveIT.getHiveConfigBean();

    return new HiveMetastoreTarget(config);
  }

}
