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
package com.streamsets.datacollector.aster;

import com.streamsets.datacollector.task.Task;
import com.streamsets.lib.security.http.aster.AsterServiceHook;

/**
 * Synchronizes entitlements with Aster.
 *
 * To trigger a synchronization immediately, call {@link #run()}.
 */
public interface EntitlementSyncTask extends Task, AsterServiceHook {

  /**
   * Immediately get the latest entitlement, and block until this task is complete.
   *
   * @return true if entitlement was changed, false otherwise
   */
  boolean syncEntitlement();
}
