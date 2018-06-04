/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.runner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.util.LambdaUtil;
import com.streamsets.pipeline.api.delegate.StageLibraryDelegate;
import com.streamsets.pipeline.api.delegate.exported.ClusterJob;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class StageLibraryDelegateRuntime implements Runnable, ClusterJob {

  // Static list with all supported delegates
  private static Set<Class> SUPPORTED_DELEGATES = ImmutableSet.of(Runnable.class, ClusterJob.class);

  /**
   * Return true if and only given delegate interface is supported by this instance.
   *
   * @param exportedInterface Service interface
   *
   * @return True if given interface is supported by this runtime
   */
  public static boolean supports(Class exportedInterface) {
    return SUPPORTED_DELEGATES.contains(exportedInterface);
  }

  private ClassLoader classLoader;
  private StageLibraryDelegate wrapped;

  public StageLibraryDelegateRuntime(
    ClassLoader classLoader,
    StageLibraryDelegate wrapped
  ) {
    this.classLoader = classLoader;
    this.wrapped = wrapped;
  }

  @VisibleForTesting
  public StageLibraryDelegate getWrapped() {
    return wrapped;
  }

  @Override // Runnable
  public void run() {
    LambdaUtil.privilegedWithClassLoader(
      classLoader,
      () -> { ((Runnable)wrapped).run(); return null; }
    );
  }

  @Override
  public Client getClient(Properties clusterProps) {
    Client client = LambdaUtil.privilegedWithClassLoader(
        classLoader,
        () -> ((ClusterJob)wrapped).getClient(clusterProps)
    );
    return new ClientImpl(client);
  }

  private class ClientImpl implements ClusterJob.Client {
    private final ClusterJob.Client client;

    public ClientImpl(ClusterJob.Client client) {
      this.client = client;
    }

    @Override
    public String createCluster(String clusterName) {
      return LambdaUtil.privilegedWithClassLoader(classLoader, () -> client.createCluster(clusterName));
    }

    @Override
    public void terminateCluster(String clusterId) {
      LambdaUtil.privilegedWithClassLoader(classLoader, () -> { client.terminateCluster(clusterId); return null; });
    }

    @Override
    public String getActiveCluster(String clusterName) {
      return LambdaUtil.privilegedWithClassLoader(classLoader, () -> client.getActiveCluster(clusterName));
    }

    @Override
    public Properties getClusterStatus(String clusterId) {
      return LambdaUtil.privilegedWithClassLoader(classLoader, () -> client.getClusterStatus(clusterId));
    }

    @Override
    public List<String> uploadJobFiles(Properties jobProps, List<File> files) throws IOException {
      return LambdaUtil.privilegedWithClassLoader(classLoader, () -> client.uploadJobFiles(jobProps, files));
    }

    @Override
    public void deleteJobFiles(Properties jobProps) throws IOException {
      LambdaUtil.privilegedWithClassLoader(classLoader, () -> {
        client.deleteJobFiles(jobProps);
        return null;
      });
    }

    @Override
    public Properties submitJob(Properties jobProps) throws IOException {
      return LambdaUtil.privilegedWithClassLoader(classLoader, () -> client.submitJob(jobProps));
    }

    @Override
    public Properties getJobStatus(Properties jobProps) throws IOException {
      return LambdaUtil.privilegedWithClassLoader(classLoader, () -> client.getJobStatus(jobProps));
    }

    @Override
    public void terminateJob(Properties jobProps) throws IOException {
      LambdaUtil.privilegedWithClassLoader(classLoader, () -> {
        client.terminateJob(jobProps);
        return null;
      });
    }
  }

}
