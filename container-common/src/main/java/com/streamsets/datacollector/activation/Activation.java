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
package com.streamsets.datacollector.activation;

import com.streamsets.datacollector.main.RuntimeInfo;

import java.util.List;
import java.util.Map;

/**
 * Contract for an activation implementation.
 *
 */
public interface Activation {

  /**
   * Activation status bean.
   */
  interface Info {
    /**
     * Return the activation type.
     *
     * @return the activation type.
     */
    String getType();

    /**
     * Returns if the activation is valid or not.
     *
     * @return if the activation is valid or not.
     */
    boolean isValid();

    /**
     * Returns the first use of the data collector in epoch millis.
     *
     * @return the first use of the data collector in epoch millis.
     */
    long getFirstUse();

    /**
     * Returns the expiration of this activation key in epoch millis.
     * -1 for unlimited and 0 for not available (no trial).
     *
     * @return the expiration of this activation key in epoch millis.
     */
    long getExpiration();

    /**
     * Returns the SDC unique ID.
     *
     * @return the SDC unique ID.
     */
    String getSdcId();

    /**
     * Returns the user information, if available in the activation.
     *
     * @return the user information.
     */
    String getUserInfo();

    /**
     * Returns the list of valid SDC IDs that can be used with this activation key.
     *
     * @return the list of valid SDC IDs
     */
    List<String> getValidSdcIds();

    /**
     * Returns any additional info provided in the activation key.
     *
     * @return any additional info provided in the activation key.
     */
    Map<String, Object> getAdditionalInfo();

  }

  /**
   * Initializes the Activation instance.
   *
   * @param runtimeInfo the data collector runtime info.
   */
  void init(RuntimeInfo runtimeInfo);

  /**
   * Returns if the implementation enforces activation or not.
   *
   * @return if the implementation enforces activation or not.
   */
  boolean isEnabled();

  /**
   * Returns the current activation information.
   * <p/>
   * A call to this method, if it is the first time ever, it should set and persist the firstUse property.
   *
   * @return the current activation information, it returns NULL if activation is not enabled.
   */
  Info getInfo();

  /**
   * Sets an activation key.
   *
   * @param activationKey the activation key to set.
   */
  void setActivationKey(String activationKey);

}
