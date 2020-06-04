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

import com.streamsets.datacollector.restapi.bean.TestConfigDefinitionBean;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import com.streamsets.pipeline.api.ConnectionVerifier;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.common.InterfaceAudience;
import com.streamsets.pipeline.common.InterfaceStability;

import java.util.ArrayList;
import java.util.List;

public class TestConnectionDef {

  public enum TestConnectionGroups implements Label {
    G1;

    @Override
    public String getLabel() {
      return "G1";
    }
  }

  @GenerateResourceBundle
  public enum TestConnectionErrors implements ErrorCode {
    TEST_CONNECTION_ERROR("Cannot connect to test destination, reason : {}"),
    ;

    private final String msg;
    TestConnectionErrors(String msg) {
      this.msg = msg;
    }

    @Override
    public String getCode() {
      return name();
    }

    @Override
    public String getMessage() {
      return msg;
    }

  }

  public class TestConnectionVerifier extends ConnectionVerifier {
    private static final String BUCKET_EXIST_PREFIX = "streamsets-test-conn-veri-";

    @ConfigDefBean(groups = "G1")
    public TestConnection connection;

    @Override
    protected List<ConfigIssue> initConnection() {
      List<Stage.ConfigIssue> issues = new ArrayList<>();
      try {
        connection.initConnection();
      } catch (Exception e) {
        issues.add(
            getContext()
                .createConfigIssue("G1", "connection", TestConnectionErrors.TEST_CONNECTION_ERROR, e.toString())
        );
      }
      return issues;
    }

    @Override
    protected void destroyConnection() {
      connection.destroy();
    }
  }

  @InterfaceAudience.LimitedPrivate
  @InterfaceStability.Unstable
  @ConnectionDef(
      label = "Test Connection",
      type = TestConnection.TYPE,
      description = "Connects to Test Connection",
      version = 1,
      upgraderDef = "upgrader/TestConnection.yaml",
      verifier = TestConnectionVerifier.class,
      supportedEngines = { ConnectionEngine.COLLECTOR, ConnectionEngine.TRANSFORMER }
  )
  @ConfigGroups(TestConnectionGroups.class)
  public class TestConnection {

    public static final String TYPE = "TEST_CON_TYPE";

    @ConfigDef(
        required = true,
        type = ConfigDef.Type.STRING,
        label = "Test Connection Host",
        description = "Test Connection Host Description",
        defaultValue = "com.streamsets.test.host",
        displayPosition = 10,
        group = "G1"
    )
    public String host;

    @ConfigDef(
        required = false,
        type = ConfigDef.Type.NUMBER,
        label = "Test Connection Port",
        description = "Test Connection Port Description",
        defaultValue = "8080",
        displayPosition = 20,
        group = "G1"
    )
    public int port;

    public void initConnection() {
      // do something with host and port
    }

    public void destroy() {
      // dispose of client
    }
  }
}
