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

public class SSOUtils {

  public static String tokenForLog(String authToken) {
    // if less than 16 then is invalid for sure, we can fully show it
    if (authToken == null) {
      authToken = "null";
    }
    String fragment = (authToken.length() < 16) ? authToken : (authToken.substring(0, 16) + "...");
    return "TOKEN:" + fragment;
  }

}
