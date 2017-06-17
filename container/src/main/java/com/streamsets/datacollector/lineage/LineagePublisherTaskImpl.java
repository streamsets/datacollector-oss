/**
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.datacollector.lineage;

import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

public class LineagePublisherTaskImpl extends AbstractTask implements LineagePublisherTask {
  private static final Logger LOG = LoggerFactory.getLogger(LineagePublisherTaskImpl.class);

  private final Configuration configuration;

  @Inject
  public LineagePublisherTaskImpl(
    Configuration configuration
  ) {
    super("Lineage Publisher Task");
    this.configuration = configuration;
  }

  @Override
  public void publishEvent(LineageEvent event) {
    // TBD
  }

  @Override
  protected void initTask() {
    LOG.info("Lineage Publisher Task is initializing");
  }

  @Override
  protected void runTask() {
    LOG.info("Lineage Publisher Task is starting");
  }

  @Override
  protected void stopTask() {
    LOG.info("Lineage Publisher Task is stopping");
  }


}
