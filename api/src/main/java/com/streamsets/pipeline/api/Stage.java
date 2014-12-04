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
package com.streamsets.pipeline.api;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.util.List;

public interface Stage<C extends Stage.Context> {

  public interface Info {

    public String getName();

    public String getVersion();

    public String getInstanceName();

  }

  public interface Context {

    public List<Info> getPipelineInfo();

    public MetricRegistry getMetrics();

    public Timer createTimer(String name);

    public Meter createMeter(String name);

    public Counter createCounter(String name);

    public void reportError(Exception exception);

    public void reportError(String errorMessage);

    public void reportError(ErrorCode errorCode, String... args);

    public void toError(Record record, Exception exception);

    public void toError(Record record, String errorMessage);

    public void toError(Record record, ErrorCode errorCode, String... args);

  }

  public void init(Info info, C context) throws StageException;

  public void destroy();

}
