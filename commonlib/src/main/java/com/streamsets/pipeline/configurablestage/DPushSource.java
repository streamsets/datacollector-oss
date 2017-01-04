/**
 * Copyright 2015 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.configurablestage;

import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;

import java.util.Map;

public abstract class DPushSource extends DStage<PushSource.Context> implements PushSource {

  protected abstract PushSource createPushSource();

  @Override
  Stage<PushSource.Context> createStage() {
    return createPushSource();
  }

  public PushSource getPushSource() {
    return (PushSource) getStage();
  }

  @Override
  public int getNumberOfThreads() {
    return getPushSource().getNumberOfThreads();
  }

  @Override
  public void produce(Map<String, String> map, int i) throws StageException {
    getPushSource().produce(map, i);
  }

}
