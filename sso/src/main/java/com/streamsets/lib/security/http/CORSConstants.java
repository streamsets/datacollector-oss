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
package com.streamsets.lib.security.http;

public class CORSConstants {
  public static final String HTTP_ACCESS_CONTROL_ALLOW_ORIGIN = "http.access.control.allow.origin";
  public static final String HTTP_ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT = "*";

  public static final String HTTP_ACCESS_CONTROL_ALLOW_HEADERS = "http.access.control.allow.headers";
  public static final String HTTP_ACCESS_CONTROL_ALLOW_HEADERS_DEFAULT =
      "origin, content-type, cache-control, accept, authorization, x-requested-by, x-ss-user-auth-token, x-ss-rest-call";

  public static final String HTTP_ACCESS_CONTROL_EXPOSED_HEADERS = "http.access.control.exposed.headers";
  public static final String HTTP_ACCESS_CONTROL_EXPOSED_HEADERS_DEFAULT = "X-SDC-LOG-PREVIOUS-OFFSET";

  public static final String HTTP_ACCESS_CONTROL_ALLOW_METHODS = "http.access.control.allow.methods";
  public static final String HTTP_ACCESS_CONTROL_ALLOW_METHODS_DEFAULT = "GET, POST, PUT, DELETE, OPTIONS, HEAD";

}
