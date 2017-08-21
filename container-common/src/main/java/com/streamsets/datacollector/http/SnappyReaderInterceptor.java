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

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.ext.ReaderInterceptor;
import javax.ws.rs.ext.ReaderInterceptorContext;
import java.io.IOException;
import java.io.InputStream;

public class SnappyReaderInterceptor implements ReaderInterceptor {

  static final String CONTENT_ENCODING = "Content-Encoding";
  static final String SNAPPY = "snappy";

  @Override
  public Object aroundReadFrom(ReaderInterceptorContext context)  throws IOException, WebApplicationException {
    if (context.getHeaders().containsKey(CONTENT_ENCODING) &&
      context.getHeaders().get(CONTENT_ENCODING).contains(SNAPPY)) {
      InputStream originalInputStream = context.getInputStream();
      context.setInputStream(new SnappyFramedInputStream(originalInputStream, true));
    }
    return context.proceed();
  }
}
