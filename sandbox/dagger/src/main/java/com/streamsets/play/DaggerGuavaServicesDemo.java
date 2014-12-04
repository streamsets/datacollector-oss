/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
