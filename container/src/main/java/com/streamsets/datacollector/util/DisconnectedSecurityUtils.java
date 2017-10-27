/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.util;

import com.streamsets.datacollector.event.dto.DisconnectedSsoCredentialsEvent;
import com.streamsets.datacollector.event.handler.remote.RemoteEventHandlerTask;
import com.streamsets.datacollector.io.DataStore;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

public class DisconnectedSecurityUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteEventHandlerTask.class);

  public static void writeDisconnectedCredentials(DataStore dataStore, DisconnectedSsoCredentialsEvent
                                            disconnectedSsoCredentialsEvent) {
    try (OutputStream os = dataStore.getOutputStream()) {
      ObjectMapperFactory.get().writeValue(os, disconnectedSsoCredentialsEvent);
      dataStore.commit(os);
    } catch (IOException ex) {
      LOG.warn(
          "Disconnected credentials maybe out of sync, could not write to '{}': {}",
          dataStore.getFile(),
          ex.toString(),
          ex
      );
    } finally {
      dataStore.release();
    }
  }

}
