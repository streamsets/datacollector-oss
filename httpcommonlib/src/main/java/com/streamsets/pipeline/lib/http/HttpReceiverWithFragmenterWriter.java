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

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.credential.CredentialValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class HttpReceiverWithFragmenterWriter implements HttpReceiver {
  private static final Logger LOG = LoggerFactory.getLogger(HttpReceiverWithFragmenterWriter.class);

  private final String uriPath;
  private final HttpConfigs httpConfigs;
  private final HttpRequestFragmenter fragmenter;
  private final FragmentWriter writer;

  public HttpReceiverWithFragmenterWriter(
      String uriPath,
      HttpConfigs httpConfigs,
      HttpRequestFragmenter fragmenter,
      FragmentWriter writer
  ) {
    this.uriPath = uriPath;
    this.httpConfigs = httpConfigs;
    this.fragmenter = fragmenter;
    this.writer = writer;
  }

  @VisibleForTesting
  public HttpRequestFragmenter getFragmenter() {
    return fragmenter;
  }

  @VisibleForTesting
  public FragmentWriter getWriter() {
    return writer;
  }

  @Override
  public List<Stage.ConfigIssue> init(Stage.Context context) {
    return new ArrayList<>();
  }

  @Override
  public void destroy() {
  }

  @Override
  public boolean isApplicationIdEnabled() {
    return httpConfigs.isApplicationIdEnabled();
  }

  @Override
  public List<? extends CredentialValue> getAppIds() {
    return httpConfigs.getAppIds();
  }

  @Override
  public boolean isAppIdViaQueryParamAllowed() {
    return httpConfigs.isAppIdViaQueryParamAllowed();
  }

  @Override
  public String getUriPath() {
    return uriPath;
  }

  @Override
  public boolean validate(HttpServletRequest req, HttpServletResponse res) throws IOException {
    return getFragmenter().validate(req, res);
  }

  @Override
  public boolean process(HttpServletRequest req, InputStream is, HttpServletResponse resp) throws IOException {
    String requestor = req.getRemoteAddr() + ":" + req.getRemotePort();
    LOG.debug("Processing request from '{}'", requestor);
    List<byte[]> fragments =
        getFragmenter().fragment(is, writer.getMaxFragmentSizeKB(), httpConfigs.getMaxHttpRequestSizeKB());
    LOG.debug("Request from '{}' broken into '{}KB' fragments for writing", requestor, fragments.size());
    getWriter().write(fragments);
    return true;
  }

}
