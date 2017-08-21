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
package com.streamsets.pipeline.lib.fragmentqueue;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;

public class TestMemoryBufferFragmentQueue {

  @Test
  public void testMemoryBufferFragmentQueueDelegation() throws Exception {
    FragmentQueue queueMock = Mockito.mock(FragmentQueue.class);
    MemoryBufferFragmentQueue memoryBufferQueue = new MemoryBufferFragmentQueue(5, queueMock);

    Stage.Context context =
        ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, ImmutableList.of("a"));

    memoryBufferQueue.init(context);
    Mockito.verify(queueMock, Mockito.times(1)).init(Mockito.eq(context));

    byte[] data = new byte[0];
    List<byte[]> list = ImmutableList.of(data);

    long now = System.currentTimeMillis();
    memoryBufferQueue.write(list);

    //lets wait till the writing thread kicks in
    while (now > memoryBufferQueue.getLastMemoryPoll());

    ArgumentCaptor<List> listCaptor = ArgumentCaptor.forClass(List.class);
    Mockito.verify(queueMock, Mockito.times(1)).write(listCaptor.capture());
    Assert.assertEquals(data, listCaptor.getValue().get(0));

    Mockito.when(queueMock.poll(Mockito.eq(1))).thenReturn(null);
    Assert.assertNull(memoryBufferQueue.poll(1));
    Mockito.verify(queueMock, Mockito.times(1)).poll(Mockito.eq(1));

    Mockito.when(queueMock.poll(Mockito.eq(1), Mockito.eq(2L))).thenReturn(null);
    Assert.assertNull(memoryBufferQueue.poll(1, 2));
    Mockito.verify(queueMock, Mockito.times(1)).poll(Mockito.eq(1), Mockito.eq(2L));

    Mockito.when(queueMock.getLostFragmentsCountAndReset()).thenReturn(1);
    Assert.assertEquals(1, memoryBufferQueue.getLostFragmentsCountAndReset());
    Mockito.verify(queueMock, Mockito.times(1)).getLostFragmentsCountAndReset();

    memoryBufferQueue.destroy();
    Mockito.verify(queueMock, Mockito.times(1)).destroy();
  }

  @Test
  public void testMemoryBufferFragmentQueueMaxSize() throws Exception {
    FragmentQueue queueMock = Mockito.mock(FragmentQueue.class);
    MemoryBufferFragmentQueue memoryBufferQueue = new MemoryBufferFragmentQueue(2, queueMock);
    memoryBufferQueue = Mockito.spy(memoryBufferQueue);

    Mockito.doReturn(new Runnable() {
      @Override
      public void run() {

      }
    }).when(memoryBufferQueue).getWriterRunnable();

    Stage.Context context =
        ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, ImmutableList.of("a"));

    memoryBufferQueue.init(context);

    byte[] data = new byte[0];
    List<byte[]> list = ImmutableList.of(data, data, data);

    memoryBufferQueue.write(list);

    Mockito.when(queueMock.getLostFragmentsCountAndReset()).thenReturn(1);
    Assert.assertEquals(2, memoryBufferQueue.getLostFragmentsCountAndReset());

    Mockito.when(queueMock.getLostFragmentsCountAndReset()).thenReturn(0);
    Assert.assertEquals(0, memoryBufferQueue.getLostFragmentsCountAndReset());

    memoryBufferQueue.destroy();
  }

}
