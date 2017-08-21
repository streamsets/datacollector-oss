/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.hive.queryexecutor;

import com.streamsets.pipeline.stage.BaseHiveIT;
import com.streamsets.pipeline.stage.lib.hive.HiveConfigBean;

import java.util.ArrayList;
import java.util.List;

public final class HiveQueryExecutorTestBuilder {
  private HiveConfigBean hiveConfigBean;
  private List<String> queries;
  private boolean stopOnQueryFailure;

  HiveQueryExecutorTestBuilder() {
    hiveConfigBean = BaseHiveIT.getHiveConfigBean();
    queries = new ArrayList<>();
    stopOnQueryFailure = true;
  }

  public HiveQueryExecutorTestBuilder hiveConfigBean(HiveConfigBean hiveConfigBean) {
    this.hiveConfigBean = hiveConfigBean;
    return this;
  }

  public HiveQueryExecutorTestBuilder addQueries(List<String> queries) {
    this.queries.addAll(queries);
    return this;
  }

  public HiveQueryExecutorTestBuilder addQuery(String query) {
    this.queries.add(query);
    return this;
  }

  public HiveQueryExecutorTestBuilder stopOnQueryFailure(boolean stopOnQueryFailure) {
    this.stopOnQueryFailure = stopOnQueryFailure;
    return this;
  }

  public HiveQueryExecutor build() {
    HiveQueryExecutorConfig config = new HiveQueryExecutorConfig();
    config.hiveConfigBean = hiveConfigBean;
    config.queries = queries;
    config.stopOnQueryFailure = stopOnQueryFailure;
    return new HiveQueryExecutor(config);
  }

}
