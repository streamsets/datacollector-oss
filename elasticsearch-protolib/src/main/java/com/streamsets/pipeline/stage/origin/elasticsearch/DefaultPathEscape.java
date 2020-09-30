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
package com.streamsets.pipeline.stage.origin.elasticsearch;

import com.streamsets.pipeline.lib.elasticsearch.PathEscape;

import java.net.URI;
import java.net.URISyntaxException;

public class DefaultPathEscape implements PathEscape {

  @Override
  public String escape(final String path) throws URISyntaxException {
    // URI is probably the most reliable way to encode URI elements.
    // java.net.URLEncoder is not suitable for the purposes since,
    // as it's described in the doc, it's an "utility for HTML form encoding"
    // not URI element encoding.
    // java.net.URI.URI(java.lang.String) may throw an exception since
    // it expects already escaped input.

    URI uri = new URI(null, null, null, -1, path, null, null);
    return uri.getRawPath();
  }
}
