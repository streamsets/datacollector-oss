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
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.ContainerClassLoader;

/**
 * This is SDC specific security manager which is a wrapper on top of JVM's default security manager.
 *
 * Regardless of what policies are configured, this implementation will prevent access to SDC's private directories
 * for all class loaders other then container (e.g. for all plugable pieces - stages, interceptors, ... - as they run
 * in their own class loaders).
 */
public class SdcSecurityManager extends SecurityManager {

  private final RuntimeInfo runtimeInfo;

  public SdcSecurityManager(RuntimeInfo runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
    // DO NOT REMOVE: This statement have side effect of loading the class which is essential for the proper
    // functionality of this class.
    Class klass = ContainerClassLoader.class;
  }

  @Override
  public void checkRead(String file) {
    checkPrivatePathsForRead(file);
    super.checkRead(file);
  }

  @Override
  public void checkRead(String file, Object context) {
    checkPrivatePathsForRead(file);
    super.checkRead(file, context);
  }

  @Override
  public void checkWrite(String file) {
    checkPrivatePathsForWrite(file);
    super.checkWrite(file);
  }

  @Override
  public void checkDelete(String file) {
    checkPrivatePathsForWrite(file);
    super.checkDelete(file);
  }

  private void checkPrivatePathsForRead(String path) {
    if(path.startsWith(runtimeInfo.getConfigDir()) || path.startsWith(runtimeInfo.getDataDir())) {
      ensureContainerClassloader(path);
    }
  }

  private void checkPrivatePathsForWrite(String path) {
    checkPrivatePathsForRead(path);
    if(path.startsWith(runtimeInfo.getConfigDir()) || path.startsWith(runtimeInfo.getDataDir()) || path.startsWith(runtimeInfo.getResourcesDir())) {
      ensureContainerClassloader(path);
    }
  }

  private void ensureContainerClassloader(String path) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    // Container class loader is the only classloader that is allowed to touch the protected directories
    if(cl instanceof ContainerClassLoader) {
        return;
    }

    throw new SecurityException(Utils.format(
      "Classloader {} is not allowed access to Data Collector internal directories ({}).",
      cl.toString(),
      path
    ));
  }

}
