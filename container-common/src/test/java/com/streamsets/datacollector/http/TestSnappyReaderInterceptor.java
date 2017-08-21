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

import org.iq80.snappy.SnappyFramedInputStream;
import org.iq80.snappy.SnappyFramedOutputStream;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.ReaderInterceptorContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class TestSnappyReaderInterceptor {

  @Test
  public void testSnappyReaderInterceptor() throws IOException {

    SnappyReaderInterceptor readerInterceptor = new SnappyReaderInterceptor();

    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    headers.put(SnappyReaderInterceptor.CONTENT_ENCODING, Arrays.asList(SnappyReaderInterceptor.SNAPPY));

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    SnappyFramedOutputStream snappyFramedOutputStream = new SnappyFramedOutputStream(byteArrayOutputStream);
    snappyFramedOutputStream.write("Hello".getBytes());
    ByteArrayInputStream inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());

    ReaderInterceptorContext mockInterceptorContext = Mockito.mock(ReaderInterceptorContext.class);
    Mockito.when(mockInterceptorContext.getHeaders()).thenReturn(headers);
    Mockito.when(mockInterceptorContext.getInputStream()).thenReturn(inputStream);
    Mockito.doNothing().when(mockInterceptorContext).setInputStream(Mockito.any(InputStream.class));

    // call aroundReadFrom on mock
    readerInterceptor.aroundReadFrom(mockInterceptorContext);

    // verify that setInputStream method was called once with argument which is an instance of SnappyFramedInputStream
    Mockito.verify(mockInterceptorContext, Mockito.times(1))
      .setInputStream(Mockito.any(SnappyFramedInputStream.class));

  }
}
