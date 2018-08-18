/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.runner;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.streamsets.datacollector.creation.InterceptorBean;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.util.LambdaUtil;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.interceptor.Interceptor;
import com.streamsets.pipeline.api.interceptor.InterceptorCreator;

import java.util.List;

/**
 * Runtime version of interceptor that wraps all methods of the interceptor to make sure that the proper classloader
 * is being used.
 */
public class InterceptorRuntime implements Interceptor {

  private final InterceptorCreator.InterceptorType type;
  private final InterceptorBean bean;
  private InterceptorContext context;

  // Metrics
  private Timer processingTimer;

  public InterceptorRuntime(
    InterceptorCreator.InterceptorType type,
    InterceptorBean bean
  ) {
    this.type = type;
    this.bean = bean;
  }

  public void setContext(InterceptorContext context) {
    this.context = context;
  }

  public InterceptorContext getContext() {
    return context;
  }

  public List<Issue> init() {
    MetricRegistry metrics = getContext().getMetrics();
    String metricsBaseName = "interceptor.stage."
      + this.type.name()
      + "."
      + this.context.getStageInstanceName()
      + "."
      + bean.getMetricName()
    ;

    processingTimer = MetricsConfigurator.createStageTimer(
      metrics,
      metricsBaseName + ".batchProcessing",
      context.getPipelineId(),
      context.getRev()
    );

    return (List)init(context);
  }

  @Override
  public List<ConfigIssue> init(Context context) {
    return LambdaUtil.privilegedWithClassLoader(
        bean.getDefinition().getStageClassLoader(),
        () -> bean.getInterceptor().init(context)
    );
  }

  @Override
  public List<Record> intercept(List<Record> records) {
    Timer.Context timerContext = processingTimer.time();
    try {
      return LambdaUtil.privilegedWithClassLoader(
        bean.getDefinition().getStageClassLoader(),
        () -> bean.getInterceptor().intercept(records)
      );
    } finally {
     timerContext.stop();
    }
  }

  @Override
  public void destroy() {
    LambdaUtil.privilegedWithClassLoader(
      bean.getDefinition().getStageClassLoader(),
      () -> { bean.getInterceptor().destroy(); return null; }
    );
  }
}
