/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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

    public String getVersion();

    public String getInstanceName();

  }

  public interface ELContext {

    public void parseEL(String el) throws ELEvalException;

    public ELVars createELVars();

    public ELEval createELEval(String configName);

  }

  public interface Context extends ELContext {

    public ExecutionMode getExecutionMode();

    /**
     * Returns the pipeline max memory in MiB
     * @return
     */
    public long getPipelineMaxMemory();

    public boolean isPreview();

    public boolean isClusterMode();

    public ConfigIssue createConfigIssue(String configGroup, String configName, ErrorCode errorCode,
        Object... args);

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

  }

  public interface ConfigIssue {
  }

  public List<ConfigIssue> validateConfigs(Info info, C context)  throws StageException;

  public void init(Info info, C context) throws StageException;

  public void destroy();

}
