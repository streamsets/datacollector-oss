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

package com.streamsets.datacollector.definition.connection;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionVerifier;
import com.streamsets.pipeline.api.ConnectionVerifierDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;

import java.util.ArrayList;
import java.util.List;

@ConnectionVerifierDef(
    verifierType = TestConnectionDef.TestConnection.TYPE,
    connectionFieldName = "connection",
    connectionSelectionFieldName = "connectionSelection"
)
public class TestConnectionVerifier extends ConnectionVerifier {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = TestConnectionDef.TestConnection.TYPE,
      defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
      label = "Connection"
  )
  @ValueChooserModel(ConnectionDef.Constants.ConnectionChooserValues.class)
  public String connectionSelection = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL;

  @ConfigDefBean(
      groups = "G1",
      dependencies = {
          @Dependency(
              configName = "connectionSelection",
              triggeredByValues = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL
          )
      }
  )
  public TestConnectionDef.TestConnection connection;

  @Override
  protected List<ConfigIssue> initConnection() {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    try {
      connection.initConnection();
    } catch (Exception e) {
      issues.add(
          getContext()
              .createConfigIssue("G1", "connection", TestConnectionDef.TestConnectionErrors.TEST_CONNECTION_ERROR, e.toString())
      );
    }
    return issues;
  }

  @Override
  protected void destroyConnection() {
    connection.destroy();
  }
}