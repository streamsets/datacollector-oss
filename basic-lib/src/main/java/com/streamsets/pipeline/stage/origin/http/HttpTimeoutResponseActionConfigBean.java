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
package com.streamsets.pipeline.stage.origin.http;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;

public class HttpTimeoutResponseActionConfigBean extends HttpResponseActionConfigBean {

    public static final ResponseAction DEFAULT_ACTION = ResponseAction.RETRY_IMMEDIATELY;

    public HttpTimeoutResponseActionConfigBean(int maxNumRetries, long backoffInterval, ResponseAction action) {
        this.maxNumRetries = maxNumRetries;
        this.backoffInterval = backoffInterval;
        this.action = action;
    }

    public HttpTimeoutResponseActionConfigBean(long backoffInterval, ResponseAction action) {
        this(-1, backoffInterval, action);
    }

    @SuppressWarnings("unused")
    public HttpTimeoutResponseActionConfigBean() {
        // needed for UI
    }

    @ConfigDef(
        required = true,
        type = ConfigDef.Type.MODEL,
        label = "Action for timeout",
        description = "Action to take when the request times out (server does not respond within read timeout" +
            " parameter)",
        group = "#0",
        defaultValue = "RETRY_IMMEDIATELY",
        displayPosition = 50,
        displayMode = ConfigDef.DisplayMode.ADVANCED
    )
    @ValueChooserModel(ResponseActionChooserValues.class)
    public ResponseAction action = DEFAULT_ACTION;

    @ConfigDef(
        required = true,
        type = ConfigDef.Type.NUMBER,
        label = "Base Backoff Interval (ms)",
        description = "Base backoff interval in milliseconds.  Defaults to 1000 (1 second).",
        defaultValue = ""+ HttpResponseActionConfigBean.DEFAULT_BACKOFF_INTERVAL_MS,
        group = "#0",
        displayPosition = 100,
        displayMode = ConfigDef.DisplayMode.ADVANCED,
        dependsOn = "action",
        triggeredByValue = { "RETRY_LINEAR_BACKOFF", "RETRY_EXPONENTIAL_BACKOFF" }
    )
    public long backoffInterval = HttpResponseActionConfigBean.DEFAULT_BACKOFF_INTERVAL_MS;

    @ConfigDef(
        required = true,
        type = ConfigDef.Type.NUMBER,
        label = "Max Retries",
        description = "The maximum number of times to retry the request before failing the stage.  A negative" +
                " value will be treated as unlimited (i.e. infinite retries).",
        defaultValue = ""+ HttpResponseActionConfigBean.DEFAULT_MAX_NUM_RETRIES,
        group = "#0",
        displayPosition = 150,
        displayMode = ConfigDef.DisplayMode.ADVANCED,
        dependsOn = "action",
        triggeredByValue = { "RETRY_LINEAR_BACKOFF", "RETRY_EXPONENTIAL_BACKOFF", "RETRY_IMMEDIATELY" }
    )
    public int maxNumRetries = HttpResponseActionConfigBean.DEFAULT_MAX_NUM_RETRIES;

    @Override
    public int getStatusCode() {
        return DUMMY_STATUS;
    }

    @Override
    public long getBackoffInterval() {
        return backoffInterval;
    }

    @Override
    public int getMaxNumRetries() {
        return maxNumRetries;
    }

    @Override
    public ResponseAction getAction() {
        return action;
    }
}
