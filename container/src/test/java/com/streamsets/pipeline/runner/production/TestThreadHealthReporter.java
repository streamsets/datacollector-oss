/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestThreadHealthReporter {
  private static final String MY_THREAD = "MyThread";
  private static final String NOT_MY_THREAD = "NotMyThread";

  private ThreadHealthReporter threadHealthReporter;
  private MetricRegistry metricRegistry = new MetricRegistry();

  @Before
  public void setUp() {
    threadHealthReporter = new ThreadHealthReporter(metricRegistry);
  }

  @Test
  public void testRegisterAndUnregister() {

    Assert.assertTrue(threadHealthReporter.register(MY_THREAD));

    Gauge<ThreadHealthReporter.ThreadHealthReport> gauge =
      (Gauge<ThreadHealthReporter.ThreadHealthReport>) MetricsConfigurator.getGauge(metricRegistry,
      ThreadHealthReporter.getHealthGaugeName(MY_THREAD));

    Assert.assertNotNull(gauge);
    Assert.assertTrue(gauge instanceof ThreadHealthReporter.ThreadHealthReportGauge);

    ThreadHealthReporter.ThreadHealthReportGauge threadHealthReportGauge =
      (ThreadHealthReporter.ThreadHealthReportGauge) gauge;
    Assert.assertNull(threadHealthReportGauge.getValue());

    //Try registering again with same name
    Assert.assertFalse(threadHealthReporter.register(MY_THREAD));

    //try unregister
    threadHealthReporter.unregister(MY_THREAD);
    Assert.assertNull(MetricsConfigurator.getGauge(metricRegistry,
      ThreadHealthReporter.getHealthGaugeName(MY_THREAD)));

  }

  @Test
  public void testRegisterAndDestroy() {

    Assert.assertTrue(threadHealthReporter.register(MY_THREAD));
    Assert.assertTrue(threadHealthReporter.register(NOT_MY_THREAD));

    Assert.assertNotNull(MetricsConfigurator.getGauge(metricRegistry,
      ThreadHealthReporter.getHealthGaugeName(MY_THREAD)));
    Assert.assertNotNull(MetricsConfigurator.getGauge(metricRegistry,
      ThreadHealthReporter.getHealthGaugeName(NOT_MY_THREAD)));

    threadHealthReporter.destroy();

    Assert.assertNull(MetricsConfigurator.getGauge(metricRegistry,
      ThreadHealthReporter.getHealthGaugeName(MY_THREAD)));
    Assert.assertNull(MetricsConfigurator.getGauge(metricRegistry,
      ThreadHealthReporter.getHealthGaugeName(NOT_MY_THREAD)));
  }

  @Test
  public void testReportHealth() {
    Assert.assertTrue(threadHealthReporter.register(MY_THREAD));
    Assert.assertTrue(threadHealthReporter.register(NOT_MY_THREAD));

    long myThreadTime = System.currentTimeMillis();
    threadHealthReporter.reportHealth(MY_THREAD, 10, myThreadTime);
    long notMyThreadTime = System.currentTimeMillis();
    threadHealthReporter.reportHealth(NOT_MY_THREAD, 20, notMyThreadTime);

    Gauge<ThreadHealthReporter.ThreadHealthReport> gauge =
      (Gauge<ThreadHealthReporter.ThreadHealthReport>) MetricsConfigurator.getGauge(metricRegistry,
        ThreadHealthReporter.getHealthGaugeName(MY_THREAD));

    Assert.assertNotNull(gauge);
    Assert.assertTrue(gauge instanceof ThreadHealthReporter.ThreadHealthReportGauge);

    ThreadHealthReporter.ThreadHealthReportGauge threadHealthReportGauge =
      (ThreadHealthReporter.ThreadHealthReportGauge) gauge;

    ThreadHealthReporter.ThreadHealthReport value = threadHealthReportGauge.getValue();
    Assert.assertNotNull(value);
    Assert.assertEquals(MY_THREAD, value.getThreadName());
    Assert.assertEquals(10, value.getScheduledDelay());
    Assert.assertEquals(myThreadTime, value.getTimestamp());

    gauge = (Gauge<ThreadHealthReporter.ThreadHealthReport>) MetricsConfigurator.getGauge(metricRegistry,
        ThreadHealthReporter.getHealthGaugeName(NOT_MY_THREAD));

    Assert.assertNotNull(gauge);
    Assert.assertTrue(gauge instanceof ThreadHealthReporter.ThreadHealthReportGauge);

    threadHealthReportGauge = (ThreadHealthReporter.ThreadHealthReportGauge) gauge;

    value = threadHealthReportGauge.getValue();
    Assert.assertNotNull(value);
    Assert.assertEquals(NOT_MY_THREAD, value.getThreadName());
    Assert.assertEquals(20, value.getScheduledDelay());
    Assert.assertEquals(notMyThreadTime, value.getTimestamp());

    //report more
    myThreadTime = System.currentTimeMillis();
    threadHealthReporter.reportHealth(MY_THREAD, 100, myThreadTime);
    notMyThreadTime = System.currentTimeMillis();
    threadHealthReporter.reportHealth(NOT_MY_THREAD, 200, notMyThreadTime);

    gauge = (Gauge<ThreadHealthReporter.ThreadHealthReport>) MetricsConfigurator.getGauge(metricRegistry,
      ThreadHealthReporter.getHealthGaugeName(MY_THREAD));

    Assert.assertNotNull(gauge);
    Assert.assertTrue(gauge instanceof ThreadHealthReporter.ThreadHealthReportGauge);

    threadHealthReportGauge = (ThreadHealthReporter.ThreadHealthReportGauge) gauge;

    value = threadHealthReportGauge.getValue();
    Assert.assertNotNull(value);
    Assert.assertEquals(MY_THREAD, value.getThreadName());
    Assert.assertEquals(100, value.getScheduledDelay());
    Assert.assertEquals(myThreadTime, value.getTimestamp());

    gauge = (Gauge<ThreadHealthReporter.ThreadHealthReport>) MetricsConfigurator.getGauge(metricRegistry,
      ThreadHealthReporter.getHealthGaugeName(NOT_MY_THREAD));

    Assert.assertNotNull(gauge);
    Assert.assertTrue(gauge instanceof ThreadHealthReporter.ThreadHealthReportGauge);

    threadHealthReportGauge = (ThreadHealthReporter.ThreadHealthReportGauge) gauge;

    value = threadHealthReportGauge.getValue();
    Assert.assertNotNull(value);
    Assert.assertEquals(NOT_MY_THREAD, value.getThreadName());
    Assert.assertEquals(200, value.getScheduledDelay());
    Assert.assertEquals(notMyThreadTime, value.getTimestamp());
  }

}
