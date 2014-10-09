/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.play;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.LinkedHashSet;

public class DaggerGuavaServicesDemo {

  public static abstract class BaseService extends AbstractIdleService {
    private String name;

    protected BaseService(String name) {
      this.name = name;
    }

    @Override
    protected void startUp() throws Exception {
      System.out.println(String.format("start service %s", name));
    }

    @Override
    protected void shutDown() throws Exception {
      System.out.println(String.format("stop service %s", name));
    }
  }

  public static class AService extends BaseService {

    @Inject
    public AService() {
      super("A");
    }
  }

  public static class BService extends BaseService {

    @Inject
    public BService() {
      super("B");
    }
  }

  public static class CService extends BaseService {

    @Inject
    public CService() {
      super("C");
    }
  }

  public static class App {
    private ServiceManager serviceManager;

    @Inject
    Iterable<Service> services;

    public void run() {
      serviceManager = new ServiceManager(services);
      serviceManager.startAsync();
      serviceManager.awaitHealthy();
      System.out.println("App running");
      serviceManager.stopAsync();
      serviceManager.awaitStopped();
    }
  }

  @Module(injects = {App.class})
  public static class AppModule {

    @Provides
    Iterable<Service> provideServices() {
      return new LinkedHashSet<Service>(Arrays.asList((Service) new AService(), new BService(), new CService()));
    }

  }

  public static void main(String[] args) throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new AppModule());
    App app = dagger.get(App.class);
    app.run();
  }

}
