/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.lib.basics;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.basics.NullTarget;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Iterator;

public class TestNullTarget {

  @Test
  @SuppressWarnings("unchecked")
  public void testProcessor() throws Exception {
    Iterator<Record> iterator = Mockito.mock(Iterator.class);
    Mockito.when(iterator.hasNext()).then(new Answer<Boolean>() {
      int count = 2;
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        return (count-- > 0);
      }
    });

    Batch batch = Mockito.mock(Batch.class);
    Mockito.when(batch.getRecords()).thenReturn(iterator);

    NullTarget target = new NullTarget();
    target.write(batch);
    Mockito.verify(batch, Mockito.times(1)).getRecords();
    Mockito.verify(iterator, Mockito.times(3)).hasNext();
    Mockito.verify(iterator, Mockito.times(2)).next();
  }

}
