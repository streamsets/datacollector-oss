/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.tokafka;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.http.HttpConfigs;
import com.streamsets.pipeline.lib.http.HttpRequestFragmenter;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTargetConfig;
import com.streamsets.testing.NetworkUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class TestToKafkaSource {

  @Test
  public void testLifecycle() throws Exception {
    HttpConfigs httpConfigs = Mockito.mock(HttpConfigs.class);
    Mockito.when(httpConfigs.getMaxConcurrentRequests()).thenReturn(10);
    Mockito.when(httpConfigs.getPort()).thenReturn(NetworkUtils.getRandomPort());
    KafkaTargetConfig kafkaConfigs = Mockito.mock(KafkaTargetConfig.class);
    final HttpRequestFragmenter fragmenter = Mockito.mock(HttpRequestFragmenter.class);
    ToKafkaSource source = new ToKafkaSource(httpConfigs, kafkaConfigs, 1) {
      @Override
      protected HttpRequestFragmenter createFragmenter() {
        return fragmenter;
      }

      @Override
      protected String getUriPath() {
        return "/";
      }
    };
    source = Mockito.spy(source);

    Source.Info info = Mockito.mock(Source.Info.class);
    Source.Context context =
        ContextInfoCreator.createSourceContext("n", false, OnRecordError.TO_ERROR, ImmutableList.of("a"));
    context = Mockito.spy(context);
    List<Stage.ConfigIssue> issues = source.init(info, context);
    Assert.assertTrue(issues.isEmpty());
    Mockito.verify(httpConfigs, Mockito.times(1)).init(Mockito.eq(context));
    Mockito.verify(kafkaConfigs, Mockito.times(1)).init(Mockito.eq(context), Mockito.anyList());
    Mockito.verify(fragmenter, Mockito.times(1)).init(Mockito.eq(context));

    Exception ex = new Exception();
    BlockingQueue<Exception> queue = new ArrayBlockingQueue<Exception>(1);
    queue.add(ex);
    Mockito.doReturn(queue).when(source).getErrorQueue();
    source.produce(null, 1, null);
    Mockito.verify(context, Mockito.times(1)).reportError(Mockito.eq(ex));

    source.destroy();
  }
}
