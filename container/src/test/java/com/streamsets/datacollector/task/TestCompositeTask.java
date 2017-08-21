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
package com.streamsets.datacollector.task;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.awaitility.Awaitility.await;

public class TestCompositeTask {
  private final List<String> initOrder = new ArrayList<String>();
  private final List<String> runOrder = new ArrayList<String>();
  private final List<String> stopOrder = new ArrayList<String>();
  private final List<String> waitWhileRunningOrder = new ArrayList<String>();

  public class SubTask extends AbstractTask {
    public SubTask(String name) {
      super(name);
    }

    @Override
    public void initTask() {
      initOrder.add(getName());
    }

    @Override
    public void runTask() {
      runOrder.add(getName());
    }

    @Override
    public void stopTask() {
      stopOrder.add(getName());
    }

    @Override
    public void waitWhileRunning() throws InterruptedException {
      super.waitWhileRunning();
      waitWhileRunningOrder.add(getName());
    }

  }

  @Test
  public void testCompositeTask() throws Exception {
    Task task1 = new SubTask("t1");
    Task task2 = new SubTask("t2");
    Task task = new CompositeTask("ct", ImmutableList.of(task1, task2), false);
    Assert.assertEquals("ct", task.getName());
    Assert.assertTrue(task.toString().contains("ct"));
    Assert.assertTrue(task.toString().contains("t1"));
    Assert.assertTrue(task.toString().contains("t2"));
    Assert.assertTrue(initOrder.isEmpty());
    Assert.assertTrue(runOrder.isEmpty());
    Assert.assertTrue(stopOrder.isEmpty());
    Assert.assertTrue(waitWhileRunningOrder.isEmpty());
    task.init();
    Assert.assertEquals(ImmutableList.of("t1", "t2"), initOrder);
    Assert.assertTrue(runOrder.isEmpty());
    Assert.assertTrue(stopOrder.isEmpty());
    Assert.assertTrue(waitWhileRunningOrder.isEmpty());
    task.run();
    Assert.assertEquals(ImmutableList.of("t1", "t2"), initOrder);
    Assert.assertEquals(ImmutableList.of("t1", "t2"), runOrder);
    Assert.assertTrue(stopOrder.isEmpty());
    Assert.assertTrue(waitWhileRunningOrder.isEmpty());
    task.stop();
    Assert.assertEquals(ImmutableList.of("t1", "t2"), initOrder);
    Assert.assertEquals(ImmutableList.of("t1", "t2"), runOrder);
    Assert.assertEquals(ImmutableList.of("t2", "t1"), stopOrder);
    Assert.assertTrue(waitWhileRunningOrder.isEmpty());
    task.waitWhileRunning();
    Assert.assertEquals(ImmutableList.of("t1", "t2"), initOrder);
    Assert.assertEquals(ImmutableList.of("t1", "t2"), runOrder);
    Assert.assertEquals(ImmutableList.of("t2", "t1"), stopOrder);
    Assert.assertEquals(ImmutableList.of("t1", "t2"), waitWhileRunningOrder);
  }

  @Test
  public void testCompositeTaskWithMonitor() throws Exception {
    Task task1 = new SubTask("t1");
    Task task2 = new SubTask("t2");
    final Task task = new CompositeTask("ct", ImmutableList.of(task1, task2), true);
    task.init();
    task.run();
    task1.stop();
    final long start = System.currentTimeMillis();
    await().until(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return !(System.currentTimeMillis() - start < 500 && task.getStatus() == Task.Status.RUNNING);
      }
    });
    Assert.assertFalse("Test Waiting for stop detection timed out", task.getStatus() == Task.Status.RUNNING);
    Assert.assertEquals(Task.Status.STOPPED, task.getStatus());
    Assert.assertEquals(Task.Status.STOPPED, task1.getStatus());
    Assert.assertEquals(Task.Status.STOPPED, task2.getStatus());
  }

}
