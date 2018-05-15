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

import com.streamsets.datacollector.creation.InterceptorBean;
import com.streamsets.datacollector.util.LambdaUtil;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.interceptor.Interceptor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Runtime version of interceptor that wraps all methods of the interceptor to make sure that the proper classloader
 * is being used.
 */
public class InterceptorRuntime implements Interceptor {

  private final InterceptorBean bean;
  private InterceptorContext context;

  public InterceptorRuntime(
    InterceptorBean bean
  ) {
    this.bean = bean;
  }

  public void setContext(InterceptorContext context) {
    this.context = context;
  }

  public List<Issue> init() {
    //TODO: Parameters should be passed in constructor similarly as we're passing StageConfiguration for stages
    return (List)init(Collections.emptyMap(), context);
  }

  @Override
  public List<ConfigIssue> init(Map<String, String> parameters, Context context) {
    return LambdaUtil.privilegedWithClassLoader(
        bean.getDefinition().getStageClassLoader(),
        () -> bean.getInterceptor().init(parameters, context)
    );
  }

  @Override
  public List<Record> intercept(List<Record> records) {
   return LambdaUtil.privilegedWithClassLoader(
        bean.getDefinition().getStageClassLoader(),
        () -> bean.getInterceptor().intercept(records)
    );
  }

  @Override
  public void destroy() {
    LambdaUtil.privilegedWithClassLoader(
      bean.getDefinition().getStageClassLoader(),
      () -> { bean.getInterceptor().destroy(); return null; }
    );
  }
}
