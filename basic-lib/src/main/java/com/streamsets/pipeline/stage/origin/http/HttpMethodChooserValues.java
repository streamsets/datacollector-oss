/*
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.http;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

/**
 * Chooser values for HTTP method configuration
 */
public class HttpMethodChooserValues extends BaseEnumChooserValues<HttpClientMode> {
  public HttpMethodChooserValues() {
    super(HttpMethod.class);
  }
}
