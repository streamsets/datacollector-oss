/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.lib.dirspooler;

import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.lib.event.EventCreator;
import com.streamsets.pipeline.lib.event.NoMoreDataEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

public class TestSpoolDirBaseContext {

  private static final int NUM_THREADS = 2;

  private SpoolDirBaseContext spoolDirBaseContext;

  private PushSource.Context contextMock;
  private BatchContext batchContextMock;
  private EventCreator eventCreatorMock;
  private EventCreator.EventBuilder eventBuilderMock;

  @Before
  public void setUp() {
    contextMock = Mockito.mock(PushSource.Context.class);
    batchContextMock = Mockito.mock(BatchContext.class);

    eventCreatorMock = Mockito.mock(EventCreator.class);
    eventBuilderMock = Mockito.mock(EventCreator.EventBuilder.class);

    spoolDirBaseContext = new SpoolDirBaseContext(contextMock, NUM_THREADS);
  }

  @After
  public void tearDown() {
    if (NoMoreDataEvent.EVENT_CREATOR == eventCreatorMock) {
      Whitebox.setInternalState(NoMoreDataEvent.class, new EventCreator.Builder(NoMoreDataEvent.NO_MORE_DATA_TAG,
          NoMoreDataEvent.VERSION
      ).withOptionalField(NoMoreDataEvent.RECORD_COUNT)
          .withOptionalField(NoMoreDataEvent.ERROR_COUNT)
          .withOptionalField(NoMoreDataEvent.FILE_COUNT)
          .build());
    }
  }

  @Test
  public void setNoMoreDataTrueSendEvent() {
    Mockito.when(eventBuilderMock.with(Mockito.anyString(), Mockito.anyInt())).thenReturn(eventBuilderMock);
    Mockito.when(eventCreatorMock.create(contextMock, batchContextMock)).thenReturn(eventBuilderMock);
    Whitebox.setInternalState(NoMoreDataEvent.class, eventCreatorMock);

    spoolDirBaseContext.setNoMoreData(0, true, batchContextMock, 1, 1, 1);
    spoolDirBaseContext.setNoMoreData(1, true, batchContextMock, 2, 2, 2);

    Mockito.verify(eventBuilderMock, Mockito.times(1)).createAndSend();
    Assert.assertTrue(spoolDirBaseContext.getNoMoreData(0));
    Assert.assertTrue(spoolDirBaseContext.getNoMoreData(1));
  }

  @Test
  public void setNoMoreDataTrueDonotSendEvent() {
    Mockito.when(eventBuilderMock.with(Mockito.anyString(), Mockito.anyInt())).thenReturn(eventBuilderMock);
    Mockito.when(eventCreatorMock.create(contextMock, batchContextMock)).thenReturn(eventBuilderMock);
    Whitebox.setInternalState(NoMoreDataEvent.class, eventCreatorMock);

    spoolDirBaseContext.setNoMoreData(1, true, batchContextMock, 2, 2, 2);

    Mockito.verify(eventBuilderMock, Mockito.times(0)).createAndSend();
    Assert.assertTrue(spoolDirBaseContext.getNoMoreData(1));
  }

  @Test
  public void setNoMoreDataFalse() {
    spoolDirBaseContext.setNoMoreData(0, false, batchContextMock, 0, 0, 0);
    Assert.assertFalse(spoolDirBaseContext.getNoMoreData(0));
  }

}
