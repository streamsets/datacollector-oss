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
package com.streamsets.datacollector.tunneling;

/**
 * Tunneling public constants.
 */
public interface TunnelingConstants {

  /**
   * Header to indicate the engine ID to the Istio to set up sticky sessions for the WebSocket connection.
   */
  String X_ENGINE_ID_HEADER_NAME = "X-SS-Engine-Id";

  /**
   * Header to indicate the uploaded file name for the file uploaded using MULTIPART_FORM_DATA.
   */
  String X_UPLOADED_FILE_NAME = "X-SS-Uploaded-File-Name";

  /**
   * The maximum size of a WebSocket Text and Binary message.
   */
  int MAX_MESSAGE_SIZE_CONFIG_DEFAULT = 50 * 1024 * 1024; // 50 MB
}
