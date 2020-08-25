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

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.googlecloud.CloudStorageCredentialsConfig;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

import java.util.List;

public class GCSOriginConfig {
    @ConfigDef(
        required = true,
        type = ConfigDef.Type.STRING,
        label = "Bucket",
        description = "Expression that will identify bucket for each record.",
        displayPosition = 20,
        displayMode = ConfigDef.DisplayMode.BASIC,
        evaluation = ConfigDef.Evaluation.IMPLICIT,
        //TODO SDC-7719
        group = "GCS"
    )
    public String bucketTemplate;

    @ConfigDef(
        required = true,
        type = ConfigDef.Type.STRING,
        label = "Common Prefix",
        description = "The common Prefix",
        displayPosition = 100,
        displayMode = ConfigDef.DisplayMode.BASIC,
        group = "GCS"
    )
    public String commonPrefix;

    @ConfigDef(
        required = true,
        type = ConfigDef.Type.STRING,
        label = "Prefix Pattern",
        description = "An Ant-style path pattern that defines the remaining portion of prefix excluding the common prefix",
        displayPosition = 100,
        displayMode = ConfigDef.DisplayMode.BASIC,
        group = "GCS"
    )
    public String prefixPattern;

    @ConfigDef(
        required = true,
        type = ConfigDef.Type.NUMBER,
        defaultValue = "1000",
        label = "Max Result Queue Size",
        group = "GCS",
        displayMode = ConfigDef.DisplayMode.ADVANCED
    )
    public int maxResultQueueSize;

    @ConfigDef(
        required = true,
        type = ConfigDef.Type.MODEL,
        label = "Data Format",
        displayPosition = 1,
        group = "DATA_FORMAT",
        displayMode = ConfigDef.DisplayMode.BASIC
    )
    @ValueChooserModel(DataFormatChooserValues.class)
    public DataFormat dataFormat;

    @ConfigDefBean(groups = {"GCS"})
    public BasicConfig basicConfig;

    @ConfigDefBean()
    public DataParserFormatConfig dataParserFormatConfig;

    @ConfigDefBean(groups = {"ERROR_HANDLING"})
    public GcsOriginErrorConfig gcsOriginErrorConfig;

    @ConfigDefBean(groups = "CREDENTIALS")
    public CloudStorageCredentialsConfig credentials = new CloudStorageCredentialsConfig();

    List<Stage.ConfigIssue> init(Stage.Context context, List<Stage.ConfigIssue> issues) {
        dataParserFormatConfig.init(
            context,
            dataFormat,
            Groups.GCS.name(),
            "gcsOriginConfig.dataParserFormatConfig",
            issues
        );
        return issues;
    }
}
