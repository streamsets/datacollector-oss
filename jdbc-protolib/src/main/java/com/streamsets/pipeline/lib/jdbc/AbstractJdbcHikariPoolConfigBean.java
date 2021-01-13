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

package com.streamsets.pipeline.lib.jdbc;

import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.jdbc.connection.common.AbstractJdbcConnection;
import com.streamsets.pipeline.lib.jdbc.multithread.DatabaseVendor;

public abstract class AbstractJdbcHikariPoolConfigBean extends HikariPoolConfigBean {

  protected abstract AbstractJdbcConnection getConnection();

  @Override
  public String getConnectionString() {
    return getConnection().connectionString;
  }

  @Override
  public DatabaseVendor getVendor() {
    return DatabaseVendor.forUrl(getConnection().connectionString);
  }

  @Override
  public CredentialValue getUsername() {
    return getConnection().username;
  }

  @Override
  public CredentialValue getPassword() {
    return getConnection().password;
  }

  @Override
  public boolean useCredentials() {
    return getConnection().useCredentials;
  }
}
