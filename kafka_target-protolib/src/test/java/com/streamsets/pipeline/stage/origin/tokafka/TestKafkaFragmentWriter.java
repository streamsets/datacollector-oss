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
package com.streamsets.pipeline.stage.origin.tokafka;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducer;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTargetConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestKafkaFragmentWriter {

  @Test
  public void testLifecycle() throws Exception {
    KafkaTargetConfig configs = new KafkaTargetConfig();
    KafkaFragmentWriter writer = new KafkaFragmentWriter(configs, 2, 10);
    writer = Mockito.spy(writer);
    writer.init(ContextInfoCreator.createSourceContext("n", false, OnRecordError.TO_ERROR, ImmutableList.of("l")));

    Assert.assertEquals(2, writer.getMaxFragmentSizeKB());

    GenericObjectPool pool = writer.getKafkaProducerPool();
    Assert.assertEquals(2, pool.getMinIdle());
    Assert.assertEquals(5, pool.getMaxIdle());
    Assert.assertEquals(10, pool.getMaxTotal());

    GenericObjectPool mockPool = Mockito.mock(GenericObjectPool.class);
    Mockito.doReturn(mockPool).when(writer).getKafkaProducerPool();

    SdcKafkaProducer producer = writer.getKafkaProducer();
    Mockito.verify(mockPool, Mockito.times(1)).borrowObject();

    writer.releaseKafkaProducer(producer);
    Mockito.verify(mockPool, Mockito.times(1)).returnObject(Mockito.eq(producer));

    writer.destroy();
    Mockito.verify(mockPool, Mockito.times(1)).close();
  }

  @Test
  public void testWrite() throws Exception {
    KafkaTargetConfig configs = new KafkaTargetConfig();
    KafkaFragmentWriter writer = new KafkaFragmentWriter(configs, 2, 10);
    writer = Mockito.spy(writer);
    writer.init(ContextInfoCreator.createSourceContext("n", false, OnRecordError.TO_ERROR, ImmutableList.of("l")));
    GenericObjectPool mockPool = Mockito.mock(GenericObjectPool.class);
    SdcKafkaProducer mockProducer = Mockito.mock(SdcKafkaProducer.class);
    Mockito.doReturn(mockProducer).when(mockPool).borrowObject();
    Mockito.doReturn(mockPool).when(writer).getKafkaProducerPool();

    byte[] msg = new byte[0];
    writer.write(ImmutableList.of(msg));
    Mockito.verify(writer, Mockito.times(1)).getKafkaProducer();
    Mockito.verify(mockProducer, Mockito.times(1)).enqueueMessage(Mockito.anyString(), Mockito.eq(msg), Mockito.any());
    Mockito.verify(mockProducer, Mockito.times(1)).write(null);
    Mockito.verify(writer, Mockito.times(1)).releaseKafkaProducer(Mockito.eq(mockProducer));

    writer.destroy();
  }


}
