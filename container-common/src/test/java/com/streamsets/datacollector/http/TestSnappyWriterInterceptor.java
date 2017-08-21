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
package com.streamsets.datacollector.http;

import org.iq80.snappy.SnappyFramedOutputStream;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.WriterInterceptorContext;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class TestSnappyWriterInterceptor {

  @Test
  public void testSnappyWriterInterceptor() throws IOException {

    SnappyWriterInterceptor writerInterceptor = new SnappyWriterInterceptor();

    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    WriterInterceptorContext mockInterceptorContext = Mockito.mock(WriterInterceptorContext.class);
    Mockito.when(mockInterceptorContext.getHeaders()).thenReturn(headers);
    Mockito.when(mockInterceptorContext.getOutputStream()).thenReturn(new ByteArrayOutputStream());
    Mockito.doNothing().when(mockInterceptorContext).setOutputStream(Mockito.any(OutputStream.class));

    // call aroundWriteTo on mock
    writerInterceptor.aroundWriteTo(mockInterceptorContext);

    // verify that setOutputStream method was called once with argument which is an instance of SnappyFramedOutputStream
    Mockito.verify(mockInterceptorContext, Mockito.times(1))
      .setOutputStream(Mockito.any(SnappyFramedOutputStream.class));

  }
}
