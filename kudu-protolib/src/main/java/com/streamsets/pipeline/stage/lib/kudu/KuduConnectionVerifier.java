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
package com.streamsets.pipeline.stage.lib.kudu;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionVerifier;
import com.streamsets.pipeline.api.ConnectionVerifierDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.stage.common.kudu.KuduConnection;
import com.streamsets.pipeline.stage.common.kudu.KuduConnectionGroups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.streamsets.pipeline.api.ConnectionDef.Constants.CONNECTION_SELECT_MANUAL;

@StageDef(
    version = 1,
    label = "Kudu Connection Verifier",
    description = "Verifies connections for Kudu",
    upgraderDef = "upgrader/KuduConnectionVerifier.yaml",
    onlineHelpRefUrl = ""
)
@HideStage(HideStage.Type.CONNECTION_VERIFIER)
@ConfigGroups(KuduConnectionGroups.class)
@ConnectionVerifierDef(
    verifierType = KuduConnection.TYPE,
    connectionFieldName = "connection",
    connectionSelectionFieldName = "connectionSelection"
)
public class KuduConnectionVerifier extends ConnectionVerifier {

  private static final Logger LOG = LoggerFactory.getLogger(KuduConnectionVerifier.class);

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = KuduConnection.TYPE,
      defaultValue = CONNECTION_SELECT_MANUAL,
      label = "Connection"
  )
  @ValueChooserModel(ConnectionDef.Constants.ConnectionChooserValues.class)
  public String connectionSelection = CONNECTION_SELECT_MANUAL;

  @ConfigDefBean(
      dependencies = @Dependency(configName = "connectionSelection", triggeredByValues = CONNECTION_SELECT_MANUAL)
  )
  public KuduConnection connection;

  private KuduAccessor accessor;

  @Override
  protected List<Stage.ConfigIssue> initConnection() {
    accessor = new KuduAccessor(connection);
    return accessor.verify(getContext());
  }

  @Override
  protected void destroyConnection() {
    accessor.close();
  }

}
