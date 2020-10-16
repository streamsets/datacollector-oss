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
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;
import com.streamsets.pipeline.api.Label;

public class TestConnectionDef {

  public enum TestConnectionGroups implements Label {
    G1("G1 Label"),
    G2("G2 Label"),
    G3("G3 Label"),
    ;

    private String label;

    TestConnectionGroups(String label) {
      this.label = label;
    }

    @Override
    public String getLabel() {
      return label;
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

  @InterfaceAudience.LimitedPrivate
  @InterfaceStability.Unstable
  @ConnectionDef(
      label = "Test Connection",
      type = TestConnection.TYPE,
      description = "Connects to Test Connection",
      version = 1,
      upgraderDef = "upgrader/TestConnection.yaml",
      supportedEngines = { ConnectionEngine.COLLECTOR, ConnectionEngine.TRANSFORMER }
  )
  @ConfigGroups(TestConnectionGroups.class)
  @HideConfigs("bufferSize")
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

    @ConfigDef(
        required = false,
        type = ConfigDef.Type.NUMBER,
        label = "Buffer Size",
        description = "This should be hidden (not yet supported)",
        displayPosition = 100,
        group = "G3"
    )
    public int bufferSize;

    public void initConnection() {
      // do something with host and port
    }

    public void destroy() {
      // dispose of client
    }
  }
}
