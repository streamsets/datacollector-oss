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
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;

import java.util.List;

public interface Stage<C extends Stage.Context> {

  public interface Info {

    public String getName();

    public int getVersion();

    public String getInstanceName();

  }

  public interface ELContext {

    public void parseEL(String el) throws ELEvalException;

    public ELVars createELVars();

    public ELEval createELEval(String configName);

    public ELEval createELEval(String configName, Class<?>... elDefClasses);

  }

  public interface Context extends ELContext {

    public ExecutionMode getExecutionMode();

    public long getPipelineMaxMemory();

    public boolean isPreview();

    public boolean isClusterMode();

    public ConfigIssue createConfigIssue(String configGroup, String configName, ErrorCode errorCode, Object... args);

    public List<Info> getPipelineInfo();

    public MetricRegistry getMetrics();

    public Timer createTimer(String name);

    public Meter createMeter(String name);

    public Counter createCounter(String name);

    public void reportError(Exception exception);

    public void reportError(String errorMessage);

    public void reportError(ErrorCode errorCode, Object... args);

    public OnRecordError getOnErrorRecord();

    public void toError(Record record, Exception exception);

    public void toError(Record record, String errorMessage);

    public void toError(Record record, ErrorCode errorCode, Object... args);

    Record createRecord(String recordSourceId);

    Record createRecord(String recordSourceId, byte[] raw, String rawMime);

    public long getLastBatchTime();

    public String getResourcesDirectory();

    public boolean isStopped();

  }

  public interface ConfigIssue {
  }

  public List<ConfigIssue> init(Info info, C context);

  public void destroy();

}
