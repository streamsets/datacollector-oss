/*
 * Copyright 2021 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.mysql;

public class DataSourceConfig {
  public final String hostname;
  public final String username;
  public final String password;
  public final int port;
  public final boolean useSSL;
  public final int serverId;

  public DataSourceConfig(
      final String hostname,
      final String username,
      final String password,
      final int port,
      final boolean useSSL,
      final int serverId
  ) {
    this.hostname = hostname;
    this.username = username;
    this.password = password;
    this.port = port;
    this.useSSL = useSSL;
    this.serverId = serverId;
  }
}
