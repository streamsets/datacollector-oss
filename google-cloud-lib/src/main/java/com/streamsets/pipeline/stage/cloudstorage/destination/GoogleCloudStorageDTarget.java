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

package com.streamsets.pipeline.stage.cloudstorage.destination;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.configurablestage.DTarget;

@StageDef(
    version = 1,
    label = "Google Cloud Storage",
    description = "Writes to google cloud storage.",
    icon = "cloud-storage-logo.png",
    producesEvents = true,
    onlineHelpRefUrl = "index.html#Destinations/GCS.html#task_vn4_nrl_nbb"
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class GoogleCloudStorageDTarget extends DTarget {

    @ConfigDefBean()
    public GCSTargetConfig gcsTargetConfig;

    @Override
    protected Target createTarget() {
        return new GoogleCloudStorageTarget(gcsTargetConfig);
    }
}
