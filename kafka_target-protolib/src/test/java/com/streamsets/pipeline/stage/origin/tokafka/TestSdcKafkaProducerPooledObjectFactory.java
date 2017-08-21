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

import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducer;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTargetConfig;
import org.apache.commons.pool2.PooledObject;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestSdcKafkaProducerPooledObjectFactory {

  @Test
  public void testFactory() throws Exception {
    SdcKafkaProducerPooledObjectFactory factory =
        new SdcKafkaProducerPooledObjectFactory(new KafkaTargetConfig(), DataFormat.BINARY);
    factory = Mockito.spy(factory);
    Mockito.doReturn(Mockito.mock(SdcKafkaProducer.class)).when(factory).createInternal();

    //wrap
    SdcKafkaProducer producer = factory.createInternal();
    PooledObject<SdcKafkaProducer> po = factory.wrap(producer);
    Assert.assertEquals(producer, po.getObject());

    //create
    producer = factory.create();
    Assert.assertNotNull(producer);
    Mockito.verify(producer, Mockito.only()).init();

    //activate
    producer = factory.createInternal();
    po = factory.wrap(producer);
    factory.activateObject(po);
    Mockito.verifyZeroInteractions(producer);

    //passivate
    producer = factory.createInternal();
    po = factory.wrap(producer);
    factory.passivateObject(po);
    Mockito.verify(producer, Mockito.times(1)).clearMessages();

    //destroy
    producer = factory.createInternal();
    po = factory.wrap(producer);
    factory.destroyObject(po);
    Mockito.verify(producer, Mockito.times(1)).destroy();
  }

}
