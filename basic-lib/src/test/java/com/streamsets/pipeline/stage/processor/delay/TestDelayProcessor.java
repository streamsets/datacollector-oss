/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.delay;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.stubbing.OngoingStubbing;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.awaitility.Awaitility.await;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(Parameterized.class)
public class TestDelayProcessor {
  private static final int DELAY = 1000;
  private static final int TIME_LAG = 500;

  @Mock
  private Batch batch;

  @Mock
  private BatchMaker maker;

  @Mock
  private Iterator<Record> it;

  @Mock
  private Record record;

  @Parameterized.Parameters
  public static Collection<?> params() {
    return Arrays.asList(new Object[][]{
        {0, false, false},
        {0, false, true},
        {0, true, false},
        {0, true, true},
        {DELAY, false, false},
        {DELAY, false, true},
        {DELAY, true, false},
        {DELAY, true, true}
    });
  }

  private DelayProcessor processor;

  private final int delay;
  private final boolean skipDelayOnEmptyBatch;
  private final boolean emptyBatch;

  public TestDelayProcessor(final int delay, final boolean skipDelayOnEmptyBatch, final boolean emptyBatch) {
    this.delay = delay;
    this.skipDelayOnEmptyBatch = skipDelayOnEmptyBatch;
    this.emptyBatch = emptyBatch;
  }

  @Before
  public void setUp() {
    initMocks(this);

    processor = new DelayProcessor();
  }

  @Test
  public void testProcess() {
    // given
    processor.delay = delay;
    processor.skipDelayOnEmptyBatch = skipDelayOnEmptyBatch;

    if (emptyBatch) {
      when(it.hasNext()).thenReturn(false);
    } else {
      when(it.next()).thenReturn(record);

      OngoingStubbing<Boolean> stubbing = when(it.hasNext());
      if (delay > 0) {
        stubbing = stubbing.thenReturn(true);
      }
      stubbing = stubbing.thenReturn(true);
      stubbing.thenReturn(false);
    }
    when(batch.getRecords()).thenReturn(it);

    // when
    await().atMost(delay + TIME_LAG, MILLISECONDS).until(() -> {
      processor.process(batch, maker);
    });

    //then
    if (emptyBatch) {
      verify(maker, never()).addRecord(any(), anyVararg());
    } else {
      verify(maker).addRecord(eq(record), anyVararg());
    }
  }
}
