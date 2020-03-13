/**
 * Copyright 2020 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.security.usermgnt;

import java.util.List;
import java.util.regex.Pattern;

public class ClearUserLine extends UserLine {

  public final static String MODE = "";

  public static final Pattern PATTERN = Pattern.compile(getUserLineRegex(MODE));

  public static final Hasher HASHER = (u, p) -> p;

  public ClearUserLine(String value) {
    super(MODE, HASHER, value);
  }

  public ClearUserLine(
      String user,
      String email,
      List<String> groups,
      List<String> roles, String password
  ) {
    super(MODE, HASHER, user, email, groups, roles, password);
  }

  @Override
  protected Pattern getPattern() {
    return PATTERN;
  }

}
