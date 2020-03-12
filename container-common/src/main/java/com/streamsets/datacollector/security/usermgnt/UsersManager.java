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
package com.streamsets.datacollector.security.usermgnt;

import java.io.IOException;
import java.util.List;

public interface UsersManager {

  String create(String user, String email, List<String> groups, List<String> roles)  throws IOException;

  User get(String user) throws IOException;

  String resetPassword(String user) throws IOException;

  void setPasswordFromReset(String user, String resetPassword, String password) throws IOException;

  void changePassword(String user, String oldPassword, String newPassword) throws IOException;

  boolean verifyPassword(String user, String password) throws IOException;

  void delete(String user) throws IOException;

  void update(String user, String email, List<String> groups, List<String> roles) throws IOException;

  List<User> listUsers() throws IOException;

  List<String> listGroups() throws IOException;
}
