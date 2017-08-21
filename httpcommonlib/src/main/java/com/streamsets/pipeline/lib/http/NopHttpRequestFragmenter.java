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
package com.streamsets.pipeline.lib.http;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class NopHttpRequestFragmenter implements HttpRequestFragmenter {

  @Override
  public List<Stage.ConfigIssue> init(Stage.Context context) {
    return new ArrayList<>();
  }

  @Override
  public void destroy() {

  }

  @Override
  public boolean validate(HttpServletRequest req, HttpServletResponse res) throws IOException {
    return true;
  }

  @Override
  public List<byte[]> fragment(InputStream is, int fragmentSizeKB, int maxSizeKB) throws IOException {
    int fragmentSizeB = fragmentSizeKB * 1000;
    int maxSizeB = maxSizeKB * 1000;
    return fragmentInternal(is, fragmentSizeB, maxSizeB);
  }

  List<byte[]> fragmentInternal(InputStream is, int fragmentSizeB, int maxSizeB) throws IOException {
    if (fragmentSizeB != maxSizeB) {
      throw new IOException(Utils.format(
          "Invalid configuration, fragmentSize '{}' and maxSize '{}' should be the same", fragmentSizeB, maxSizeB));
    }
    byte[] buffer = new byte[fragmentSizeB];
    int read = ByteStreams.read(is, buffer, 0, fragmentSizeB);
    if (is.read() > -1) {
      throw new IOException(Utils.format("Maximum data size '{}' exceeded", maxSizeB));
    }
    byte[] data = buffer;
    if (read < buffer.length) {
      data = new byte[read];
      System.arraycopy(buffer, 0, data, 0, read);
    }
    return ImmutableList.of(data);
  }
}
