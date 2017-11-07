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

package com.streamsets.pipeline.stage.cloudstorage.origin;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DSource;

@StageDef(
    version = 1,
    label = "Google Cloud Storage",
    description = "Reads from Google Cloud Storage",
    icon = "cloud-storage-logo.png",
    recordsByRef = true,
    resetOffset = true,
    producesEvents = true,
    onlineHelpRefUrl = "index.html#Origins/GCS.html#task_wzm_2rl_nbb"
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class GoogleCloudStorageDSource extends DSource {
    @ConfigDefBean()
    public GCSOriginConfig gcsOriginConfig;

    @Override
    protected Source createSource() {
        return new GoogleCloudStorageSource(gcsOriginConfig);
    }
}
