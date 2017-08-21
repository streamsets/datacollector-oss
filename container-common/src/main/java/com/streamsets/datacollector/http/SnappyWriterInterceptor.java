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

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;
import java.io.IOException;
import java.io.OutputStream;

public class SnappyWriterInterceptor implements WriterInterceptor {

  static final String CONTENT_ENCODING = "Content-Encoding";
  static final String SNAPPY = "snappy";

  @Override
  public void aroundWriteTo(WriterInterceptorContext context) throws IOException, WebApplicationException {
    context.getHeaders().add(CONTENT_ENCODING, SNAPPY);
    final OutputStream outputStream = context.getOutputStream();
    context.setOutputStream(new SnappyFramedOutputStream(outputStream));
    context.proceed();
  }
}
