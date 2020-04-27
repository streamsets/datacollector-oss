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
package com.streamsets.datacollector.usagestats;

import org.junit.Assert;
import org.junit.Test;

public class TestUsageTimer {

  @Test
  public void testProperties() throws Exception {
    UsageTimer ut = new UsageTimer().setName("name").setAccumulatedTime(5);
    Assert.assertEquals("name", ut.getName());
    Assert.assertEquals(5, ut.getAccumulatedTime());
    Assert.assertEquals(0, ut.getMultiplier());
  }

  @Test
  public void testRealClock() throws Exception {
    UsageTimer ut = new UsageTimer().setName("name");
    ut.start();
    Thread.sleep(2);
    ut.stop();
    Assert.assertTrue(ut.getAccumulatedTime() >= 2);
  }

  @Test
  public void testStartStopMultiplier() throws Exception {
    UsageTimer ut = new UsageTimer().setName("name");

    UsageTimer.doInStopTime(1, () -> {
      Assert.assertTrue(ut.getAccumulatedTime() == 0);
      Assert.assertTrue(ut.getAccumulatedTime() == 0);
      ut.start();
      Assert.assertEquals(1, ut.getMultiplier());
      return null;
    });

    UsageTimer.doInStopTime(2, () -> {
      Assert.assertEquals(2 - 1, ut.getAccumulatedTime());

      ut.start();
      Assert.assertEquals(2, ut.getMultiplier());
      return null;
    });

    UsageTimer.doInStopTime(3, () -> {
      Assert.assertEquals((2 - 1) + 2 * ( 3 - 2), ut.getAccumulatedTime());

      ut.stop();
      Assert.assertEquals(1, ut.getMultiplier());
      return null;
    });

    UsageTimer.doInStopTime(4, () -> {
      Assert.assertEquals((2 - 1) + 2 * ( 3 - 2) + (4 - 3), ut.getAccumulatedTime());

      ut.stop();
      Assert.assertEquals(0, ut.getMultiplier());

      Assert.assertEquals((2 - 1) + 2 * ( 3 - 2) + (4 - 3), ut.getAccumulatedTime());
      return null;
    });

  }

  @Test
  public void testStartIfNotRunning() throws Exception {
    UsageTimer ut = new UsageTimer().setName("name");
    Assert.assertEquals(0, ut.getMultiplier());
    Assert.assertTrue(ut.startIfNotRunning());
    Assert.assertEquals(1, ut.getMultiplier());
    Assert.assertFalse(ut.startIfNotRunning());
    Assert.assertEquals(1, ut.getMultiplier());
    Assert.assertTrue(ut.isRunning());
  }

  @Test
  public void testStopIfRunning() throws Exception {
    UsageTimer ut = new UsageTimer().setName("name");
    Assert.assertEquals(0, ut.getMultiplier());
    Assert.assertFalse(ut.stopIfRunning());
    Assert.assertEquals(0, ut.getMultiplier());
    Assert.assertFalse(ut.isRunning());
    ut.start();
    Assert.assertEquals(1, ut.getMultiplier());
    Assert.assertTrue(ut.stopIfRunning());
    Assert.assertEquals(0, ut.getMultiplier());
    Assert.assertFalse(ut.stopIfRunning());
    Assert.assertEquals(0, ut.getMultiplier());
    Assert.assertFalse(ut.isRunning());
  }

  @Test
  public void testRoll() throws Exception {
    UsageTimer ut = new UsageTimer().setName("name").setAccumulatedTime(10);

    UsageTimer.doInStopTime(1, () -> {
      ut.start();

      Assert.assertEquals(1, ut.getMultiplier());
      return null;
    });

    UsageTimer.doInStopTime(2, () -> {
      UsageTimer ut1 = ut.roll();

      Assert.assertEquals("name", ut1.getName());
      Assert.assertEquals(1, ut1.getMultiplier());
      Assert.assertEquals(0, ut1.getAccumulatedTime());

      Assert.assertEquals(0, ut.getMultiplier());
      Assert.assertEquals(10 + (2 - 1), ut.getAccumulatedTime());

      Assert.assertFalse(ut.isRunning());
      Assert.assertTrue(ut1.isRunning());
      return null;
    });

  }


  @Test
  public void testSnapshot() throws Exception {
    UsageTimer ut = new UsageTimer().setName("name").setAccumulatedTime(10);

    UsageTimer.doInStopTime(1, () -> {
      ut.start();
      return null;
    });

    UsageTimer snapshot = UsageTimer.doInStopTime(2, () -> {
      UsageTimer snapshot1 = ut.snapshot();

      Assert.assertEquals(1, ut.getMultiplier());

      Assert.assertEquals("name", snapshot1.getName());
      Assert.assertEquals(0, snapshot1.getMultiplier());
      return snapshot1;
    });

    UsageTimer.doInStopTime(3, () -> {
      Assert.assertEquals(10 + (2 - 1), snapshot.getAccumulatedTime());
      Assert.assertEquals(10 + (3 - 1), ut.getAccumulatedTime());
      return null;
    });

  }

}
