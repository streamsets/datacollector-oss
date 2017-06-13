/**
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
package com.streamsets.pipeline.lib.el;

import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.ext.DataCollectorServices;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class VaultEL {
  public static final String PREFIX = "vault";
  private static final String VAULT_SERVICE_KEY = "com.streamsets.datacollector.vault";

  private static final Method READ_WITH_DELAY;

  static {
    DataCollectorServices services = DataCollectorServices.instance();
    Object vault = services.get(VAULT_SERVICE_KEY);
    Class<?> c = vault.getClass();
    try {
      READ_WITH_DELAY = c.getDeclaredMethod("read", String.class, String.class, long.class);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e); // NOSONAR
    }
  }

  private VaultEL() {}

  @ElFunction(
      prefix = PREFIX,
      name = "read",
      description = "Retrieves the value of the specified path in Vault."
  )
  public static String read(@ElParam("path") String path, @ElParam("key") String key) {
    return read(path, key, 0);
  }

  @ElFunction(
      prefix = PREFIX,
      name = "readWithDelay",
      description = "Retrieves the value of the specified path in Vault and waits for delay milliseconds." +
          "Primarily for AWS since generated credentials can take 5-10 seconds before they are ready for use."
  )
  public static String read(@ElParam("path") String path, @ElParam("key") String key, @ElParam("delay") long delay) {
    try {
      Object vault = DataCollectorServices.instance().get(VAULT_SERVICE_KEY);
      Object result = READ_WITH_DELAY.invoke(vault, path, key, delay);
      return (String) result;
    } catch (IllegalAccessException | InvocationTargetException e) {
      Throwable err = e;
      if (e.getCause() != null) {
        err = e.getCause();
      }
      throw Throwables.propagate(err);
    }
  }
}
