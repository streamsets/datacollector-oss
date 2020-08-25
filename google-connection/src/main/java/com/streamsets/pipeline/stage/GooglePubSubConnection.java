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

package com.streamsets.pipeline.stage;

import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;
import com.streamsets.pipeline.stage.common.AbstractGoogleConnection;
import com.streamsets.pipeline.stage.common.GoogleConnectionGroups;

@ConnectionDef(label = "Google Pub Sub",
    type = GooglePubSubConnection.TYPE,
    description = "Connects to Google Pub Sub",
    version = 1,
    upgraderDef = "upgrader/GooglePubSubConnection.yaml",
    supportedEngines = {ConnectionEngine.COLLECTOR, ConnectionEngine.TRANSFORMER})
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
@ConfigGroups(GoogleConnectionGroups.class)
public class GooglePubSubConnection extends AbstractGoogleConnection {
  public static final String TYPE = "STREAMSETS_GOOGLE_PUB_SUB";
}
