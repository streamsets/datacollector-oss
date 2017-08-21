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
package com.streamsets.pipeline.stage.origin.httptokafka;

import com.streamsets.pipeline.lib.http.HttpConfigs;
import com.streamsets.pipeline.lib.http.NopHttpRequestFragmenter;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTargetConfig;
import com.streamsets.pipeline.stage.origin.tokafka.HttpServerToKafkaSource;

import java.util.List;

public class HttpToKafkaSource extends HttpServerToKafkaSource {

  public static final String HTTP_PATH = "/";

  public HttpToKafkaSource(HttpConfigs httpConfigs, KafkaTargetConfig kafkaConfigs, int kafkaMaxMessageSizeKB) {
    super(HTTP_PATH, httpConfigs, new NopHttpRequestFragmenter(), kafkaConfigs, kafkaMaxMessageSizeKB);
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = getHttpConfigs().init(getContext());
    issues.addAll(getFragmenter().init(getContext()));
    getKafkaConfigs().init(getContext(), issues);
    if (issues.isEmpty()) {
      issues.addAll(super.init());
    }
    return issues;
  }

  @Override
  public void destroy() {
    super.destroy();
    getFragmenter().destroy();
    getKafkaConfigs().destroy();
    getHttpConfigs().destroy();
  }

}
