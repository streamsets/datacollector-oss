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
package com.streamsets.datacollector.util;

public class AuthzRole {
  public static final String GUEST = "guest";
  public static final String MANAGER = "manager";
  public static final String CREATOR = "creator";
  public static final String ADMIN = "admin";
  public static final String ADMIN_ACTIVATION = "admin-activation";

  public static final String GUEST_REMOTE = "datacollector:guest";
  public static final String MANAGER_REMOTE = "datacollector:manager";
  public static final String CREATOR_REMOTE = "datacollector:creator";
  public static final String ADMIN_REMOTE = "datacollector:admin";

  public static final String[] ALL_ROLES = {
      ADMIN, CREATOR, MANAGER, GUEST
  };

  private AuthzRole() {}
}
