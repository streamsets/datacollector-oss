/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.task;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

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
    Task task = new CompositeTask("ct", ImmutableList.of(task1, task2), true);
    task.init();
    task.run();
    task1.stop();
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < 500 && task.getStatus() == Task.Status.RUNNING) {
      Thread.sleep(50);
    }
    Assert.assertFalse("Test Waiting for stop detection timed out", task.getStatus() == Task.Status.RUNNING);
    Assert.assertEquals(Task.Status.STOPPED, task.getStatus());
    Assert.assertEquals(Task.Status.STOPPED, task1.getStatus());
    Assert.assertEquals(Task.Status.STOPPED, task2.getStatus());
  }

}
