/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.destination.influxdb;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.stage.lib.influxdb.InfluxBatchWriter;
import org.influxdb.InfluxDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class InfluxTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(InfluxTarget.class);
  private final String influxUrl;
  private final String influxDatabase;
  private final String username;
  private final String password;

  private InfluxBatchWriter influxWriter = null;

  public InfluxTarget(String influxUrl, String influxDatabase, String username, String password) {
    this.influxUrl = influxUrl;
    this.influxDatabase = influxDatabase;
    this.username = username;
    this.password = password;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    influxWriter = new InfluxBatchWriter(
        influxUrl,
        username,
        password,
        influxDatabase,
        InfluxDB.ConsistencyLevel.ALL,
        "default"
    );

    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    try {
      influxWriter.writeBatch(batch);
    } catch (OnRecordErrorException e) {
      // todo
    }
  }
}
