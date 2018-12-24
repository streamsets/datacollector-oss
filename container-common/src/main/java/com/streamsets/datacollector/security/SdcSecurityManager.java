/**
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.security;

import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.SDCClassLoader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.ContainerClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.util.SecurityConstants;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This is SDC specific security manager which is a wrapper on top of JVM's default security manager.
 *
 * Regardless of what policies are configured, this implementation will prevent access to SDC's private directories
 * with few exceptions:
 *
 * * Container class loader is unrestricted.
 * * SDC Configuration can specify exceptions to specific files (globally or for given stage libraries).
 *
 * There are sadly bugs in JVM when using custom security manager:
 *
 * https://bugs.openjdk.java.net/browse/JDK-8166366 (and family of repeating regressions):
 *
 * This manager is trying to bypass them all:
 *
 * * We're actively catching StackOverflowException and refreshing permissions in such scenario. This should
 *   re-initialize the proper protection domain and thus the subsequent call to super should be successful. We don't
 *   do this recursively to avoid introducing yet another problem. If even after the refresh of permissions we
 *   get StackOverflowException, well, that's it.
 */
public class SdcSecurityManager extends SecurityManager {
  Logger LOG = LoggerFactory.getLogger(SdcSecurityManager.class);

  public static final String PROPERTY_EXCEPTIONS = "security_manager.sdc_dirs.exceptions";
  public static final String PROPERTY_STAGE_EXCEPTIONS = "security_manager.sdc_dirs.exceptions.lib.";

  private final Set<String> exceptions;
  private final Map<String, Set<String>> stageLibExceptions;

  private final String configDir;
  private final String dataDir;
  private final String resourcesDir;

  public SdcSecurityManager(
    RuntimeInfo runtimeInfo,
    Configuration configuration
  ) {
    this.exceptions = new HashSet<>();
    this.stageLibExceptions = new HashMap<>();

    // DO NOT REMOVE and DO NOT CACHE THE RuntimeInfo OBJECT. Albeit your favorite CS course would claim that we should
    // hold the reference to the runtimeInfo class and call those methods, we actually do need to resolve actual values
    // and have then in memory - resolving the values during permission check itself can lead to a infinite loop - where
    // in order to resolve permission, we need need to load file from disk which on it's own requires permission check,
    // which requires permission check, ... .
    this.configDir = runtimeInfo.getConfigDir();
    this.dataDir = runtimeInfo.getDataDir();
    this.resourcesDir = runtimeInfo.getResourcesDir();

    // DO NOT REMOVE: The following statements have important side effect - they load both those classes to memory which
    // is essential for the checks below.
    Class containerClassLoader = ContainerClassLoader.class;
    Class sdcClassLoader = SDCClassLoader.class;

    // Finally load exceptions from the configuration
    setExceptions(configuration);
  }

  /**
   * This method should be called only once and before any stages are loaded.
   */
  private void setExceptions(Configuration configuration) {
    this.exceptions.clear();
    this.stageLibExceptions.clear();

    // Load general exceptions
    for(String path : configuration.get(PROPERTY_EXCEPTIONS, "").split(",")) {
      this.exceptions.add(replaceVariables(path));
    }

    // Load Stage library specific exceptions
    Configuration stageSpecific = configuration.getSubSetConfiguration(PROPERTY_STAGE_EXCEPTIONS, true);
    for(Map.Entry<String, String> entry : stageSpecific.getValues().entrySet()) {
      Set<String> stageExceptions = new HashSet<>();
      for(String path : entry.getValue().split(",")) {
        stageExceptions.add(replaceVariables(path));
      }

      this.stageLibExceptions.put(entry.getKey(), stageExceptions);
    }
  }

  /**
   * Replace variables to internal SDC directories so that users don't have to be entering FQDN.
   */
  private String replaceVariables(String path) {
    return path.replace("$SDC_DATA", dataDir)
      .replace("$SDC_CONF", configDir)
      .replace("$SDC_RESOURCES", resourcesDir)
      ;
  }

  @Override
  public void checkRead(String file) {
    try {
      checkPrivatePathsForRead(file);
      super.checkRead(file);
    } catch (StackOverflowError e) {
      LOG.error("StackOverflowError in JVM's security manager for file: {}", file, e);
      refreshPermissions();
      super.checkRead(file);
    }
  }

  @Override
  public void checkRead(String file, Object context) {
    try {
      checkPrivatePathsForRead(file);
      super.checkRead(file, context);
    } catch (StackOverflowError e) {
      LOG.error("StackOverflowError in JVM's security manager for file: {}", file, e);
      refreshPermissions();
      super.checkRead(file, context);
    }
  }

  @Override
  public void checkWrite(String file) {
    try {
      checkPrivatePathsForWrite(file);
      super.checkWrite(file);
    } catch (StackOverflowError e) {
      LOG.error("StackOverflowError in JVM's security manager for file: {}", file, e);
      refreshPermissions();
      super.checkWrite(file);
    }
  }

  @Override
  public void checkDelete(String file) {
    try {
      checkPrivatePathsForWrite(file);
      super.checkDelete(file);
    } catch (StackOverflowError e) {
      LOG.error("StackOverflowError in JVM's security manager for file: {}", file, e);
      refreshPermissions();
      super.checkDelete(file);
    }
  }

  /**
   * This method refreshes permissions for this security manager - in case that JVM was
   * kind enough and GC'ed them for us. The implementation is based on System.setSecurityManager
   * that does something very similar.
   */
  private void refreshPermissions() {
     AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
       SdcSecurityManager.class.getProtectionDomain().implies(SecurityConstants.ALL_PERMISSION);
       return null;
     });
  }

  private void checkPrivatePathsForRead(String path) {
    if(path.startsWith(configDir) || path.startsWith(dataDir)) {
      ensureProperPermissions(path);
    }
  }

  private void checkPrivatePathsForWrite(String path) {
    checkPrivatePathsForRead(path);
    if(path.startsWith(configDir) || path.startsWith(dataDir) || path.startsWith(resourcesDir)) {
      ensureProperPermissions(path);
    }
  }

  /**
   * Make sure that the active code have proper rights to access the file inside protected directory.
   */
  private void ensureProperPermissions(String path) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    // 1) Container can access anything
    if(cl instanceof ContainerClassLoader) {
        return;
    }

    // 2. Some files are whitelisted globally for all stage libraries
    if(exceptions.contains(path)) {
      return;
    }

    // 3. Some stage libraries have some files whitelisted globally
    if(cl instanceof SDCClassLoader) {
      String libraryName = ((SDCClassLoader)cl).getName();
      if(stageLibExceptions.containsKey(libraryName) && stageLibExceptions.get(libraryName).contains(path)) {
        return;
      }
    }

    // No whitelist, no fun, go away
    throw new SecurityException(Utils.format(
      "Classloader {} is not allowed access to Data Collector internal directories ({}).",
      cl.toString(),
      path
    ));
  }

}
