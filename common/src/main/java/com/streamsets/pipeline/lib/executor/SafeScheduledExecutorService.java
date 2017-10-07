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
package com.streamsets.pipeline.lib.executor;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.security.GroupsInScope;
import com.streamsets.pipeline.lib.log.LogConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SafeScheduledExecutorService extends ScheduledThreadPoolExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(SafeScheduledExecutorService.class);
  private final String name;
  private ExecutorSupport executorSupport = new ExecutorSupport(LOG);

  public SafeScheduledExecutorService(int corePoolSize, final String name) {
    this(corePoolSize, name, new ThreadFactory() {
      private final ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = defaultThreadFactory.newThread(r);
        thread.setDaemon(true);
        thread.setName(name + "-" + thread.getName());
        return thread;
      }
    });
  }

  public SafeScheduledExecutorService(int corePoolSize, String name, ThreadFactory threadFactory) {
    super(corePoolSize, threadFactory);
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public Future<?> submit(final Runnable runnable) {
    String user = MDC.get(LogConstants.USER);
    String entity = MDC.get(LogConstants.ENTITY);
    return super.submit(new SafeRunnable(user, entity, runnable, true));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
    String user = MDC.get(LogConstants.USER);
    String entity = MDC.get(LogConstants.ENTITY);
    List<SafeCallable<T>> wrappedTasks = new ArrayList<>(tasks.size());
    for (Callable<T> task : tasks) {
      wrappedTasks.add(new SafeCallable<T>(user, entity, task, true));
    }
    return super.invokeAll(wrappedTasks);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
    throws InterruptedException {
    String user = MDC.get(LogConstants.USER);
    String entity = MDC.get(LogConstants.ENTITY);
    List<SafeCallable<T>> wrappedTasks = new ArrayList<>(tasks.size());
    for (Callable<T> task : tasks) {
      wrappedTasks.add(new SafeCallable<T>(user, entity, task, true));
    }
    return super.invokeAll(wrappedTasks, timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
    String user = MDC.get(LogConstants.USER);
    String entity = MDC.get(LogConstants.ENTITY);
    List<SafeCallable<T>> wrappedTasks = new ArrayList<>(tasks.size());
    for (Callable<T> task : tasks) {
      wrappedTasks.add(new SafeCallable<T>(user, entity, task, true));
    }
    return super.invokeAny(wrappedTasks);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
    throws InterruptedException, ExecutionException, TimeoutException {
    String user = MDC.get(LogConstants.USER);
    String entity = MDC.get(LogConstants.ENTITY);
    List<SafeCallable<T>> wrappedTasks = new ArrayList<>(tasks.size());
    for (Callable<T> task : tasks) {
      wrappedTasks.add(new SafeCallable<T>(user, entity, task, true));
    }
    return super.invokeAny(wrappedTasks, timeout, unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    String user = MDC.get(LogConstants.USER);
    String entity = MDC.get(LogConstants.ENTITY);
    return super.submit(new SafeCallable<>(user, entity, task, true));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    String user = MDC.get(LogConstants.USER);
    String entity = MDC.get(LogConstants.ENTITY);
    return super.submit(new SafeRunnable(user, entity, task, true), result);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
    String user = MDC.get(LogConstants.USER);
    String entity = MDC.get(LogConstants.ENTITY);
    return super.scheduleAtFixedRate(new SafeRunnable(user, entity, command, true), initialDelay, period, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long period, TimeUnit unit) {
    String user = MDC.get(LogConstants.USER);
    String entity = MDC.get(LogConstants.ENTITY);
    return super.scheduleWithFixedDelay(new SafeRunnable(user, entity, command, true), initialDelay, period, unit);
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    String user = MDC.get(LogConstants.USER);
    String entity = MDC.get(LogConstants.ENTITY);
    return super.schedule(new SafeRunnable(user, entity, command, true), delay, unit);
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    String user = MDC.get(LogConstants.USER);
    String entity = MDC.get(LogConstants.ENTITY);
    return super.schedule(new SafeCallable<>(user, entity, callable, true), delay, unit);
  }

  @Override
  public void execute(Runnable command) {
    String user = MDC.get(LogConstants.USER);
    String entity = MDC.get(LogConstants.ENTITY);
    super.execute(new SafeRunnable(user, entity, command, false));
  }

  private static String getAsyncUserName(String user) {
    //the '*' indicates that this is run asynchronously on behalf of the user
    //we need to check if we are already async (have a '*') then do nothing, the memory check reschedules itself
    return (user == null) ? "*?" : (user.startsWith("*")) ? user : "*" + user;
  }

  private static String getEntity(String entity) {
    return (entity == null) ? "-" : entity;
  }

  private class SafeRunnable implements Runnable {
    private final Callable<Object> callable;


    public SafeRunnable(String user, String entity, Runnable delegate, boolean propagateErrors) {
      callable = new SafeCallable<>(user, entity, Executors.callable(delegate), propagateErrors);
    }

    @Override
    public void run() {
      try {
        callable.call();
      } catch (Throwable throwable) {
        if (throwable instanceof RuntimeException) {
          throw (RuntimeException)throwable;
        } else if (throwable instanceof Error) {
          throw (Error)throwable;
        } else {
          throw new RuntimeException(throwable);
        }
      }
    }
  }

  private class SafeCallable<T> implements Callable<T> {
    private final Set<String> groups;
    private final String user;
    private final String entity;
    private final Callable<T> delegate;
    private final String delegateName;
    private final boolean propagateErrors;

    public SafeCallable(
        String user, String entity, Callable<T> delegate, boolean propagateErrors
    ) {
      groups = GroupsInScope.getUserGroupsInScope();
      this.user = user;
      this.entity = entity;
      this.delegate = delegate;
      this.delegateName = delegate.toString(); // call toString() in caller thread in case of error
      this.propagateErrors = propagateErrors;
    }

    @Override
    public T call() throws Exception {
      return GroupsInScope.execute(groups, () -> {
        MDC.put(LogConstants.USER, getAsyncUserName(user));
        MDC.put(LogConstants.ENTITY, getEntity(entity));
        try {
          return delegate.call();
        } catch (Throwable throwable) {
          executorSupport.uncaughtThrowableInCallable(throwable, delegate, delegateName);
          if (propagateErrors) {
            //Not wrapping in Runtime Exception as it could be StageException when preview validation fails or
            //PipelineRuntimeException when running preview fails.
            throw throwable;
          }
          return null;
        } finally {
          MDC.clear();
        }
      });
    }
  }

  @VisibleForTesting
  void setExecutorSupport(ExecutorSupport executorSupport) {
    this.executorSupport = executorSupport;
  }

  public void scheduleAtFixedRateAndForget(Runnable command, long initialDelay, long period, TimeUnit unit) {
    String user = MDC.get(LogConstants.USER);
    String entity = MDC.get(LogConstants.ENTITY);
    super.scheduleAtFixedRate(new SafeRunnable(user, entity, command, false), initialDelay, period, unit);
  }

  public void submitAndForget(final Runnable runnable) {
    String user = MDC.get(LogConstants.USER);
    String entity = MDC.get(LogConstants.ENTITY);
    super.submit(new SafeRunnable(user, entity, runnable, false));
  }

  public void scheduleWithFixedDelayAndForget(Runnable command, long initialDelay, long period, TimeUnit unit) {
    String user = MDC.get(LogConstants.USER);
    String entity = MDC.get(LogConstants.ENTITY);
    super.scheduleWithFixedDelay(new SafeRunnable(user, entity, command, false), initialDelay, period, unit);
  }

  public void scheduleAndForget(Runnable command, long delay, TimeUnit unit) {
    String user = MDC.get(LogConstants.USER);
    String entity = MDC.get(LogConstants.ENTITY);
    super.schedule(new SafeRunnable(user, entity, command, false), delay, unit);
  }

}
