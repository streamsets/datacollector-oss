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

import java.util.List;
import java.util.Map;

public interface Stage<C extends Stage.Context> {

  public interface Info {

    public String getName();

    public String getVersion();

    public String getInstanceName();

  }

  public interface ElEvalProvider {

    public ELEval createELEval(String configName, Class<?> ... elFuncConstDefClasses);

    public ELEval.Variables parseConstants(Map<String,?> constants, Stage.Context context, String group,
                                    String config, ErrorCode err, List<Stage.ConfigIssue> issues);

    public ELEval.Variables getDefaultVariables();

    public ELEval.Variables createVariables(Map<String, Object> variables, Map<String, Object> contextVariables);

    public void validateExpression(ELEval elEvaluator, ELEval.Variables variables, String expression,
                                          Stage.Context context, String group, String config, ErrorCode err,
                                          Class<?> type, List<Stage.ConfigIssue> issues);
  }

  public interface Context extends ElEvalProvider {

    public boolean isPreview();

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

  }

  public interface ConfigIssue {
  }

  public List<ConfigIssue> validateConfigs(Info info, C context)  throws StageException;

  public List<ELEval> getElEvals(ElEvalProvider elEvalProvider);

  public void init(Info info, C context) throws StageException;

  public void destroy();

}
