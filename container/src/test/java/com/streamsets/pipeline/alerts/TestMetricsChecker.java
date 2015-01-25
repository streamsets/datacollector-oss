/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.alerts;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.config.MetricDefinition;
import com.streamsets.pipeline.config.MetricType;
import com.streamsets.pipeline.el.ELBasicSupport;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.el.ELStringSupport;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMetricsChecker {

  private static final String LANE = "lane";

  private static MetricRegistry metrics;
  private static ELEvaluator elEvaluator;
  private static ELEvaluator.Variables variables;

  @BeforeClass
  public static void setUp() {
    metrics = new MetricRegistry();
    variables = new ELEvaluator.Variables();
    elEvaluator = new ELEvaluator();
    ELBasicSupport.registerBasicFunctions(elEvaluator);
    ELRecordSupport.registerRecordFunctions(elEvaluator);
    ELStringSupport.registerStringFunctions(elEvaluator);
  }

  @Test
  public void testMeter() {
    MetricDefinition metricDefinition = new MetricDefinition("testMeter", "testMeter", "testMeter",
      "${record:value(\"/name\")==null}", "blah", MetricType.METER, true);
    MetricsChecker metricsChecker = new MetricsChecker(metricDefinition, metrics, variables, elEvaluator);
    metricsChecker.recordMetrics(TestUtil.createSnapshot("testMeter"));
    Meter meter = MetricsConfigurator.getMeter(metrics, metricDefinition.getId());
    Assert.assertNotNull(meter);
    Assert.assertEquals(3, meter.getCount());
  }

  @Test
  public void testMeterDisabled() {
    MetricDefinition metricDefinition = new MetricDefinition("testMeterDisabled", "testMeterDisabled", "testMeterDisabled",
      "${record:value(\"/name\")==null}", "blah", MetricType.METER, false);
    MetricsChecker metricsChecker = new MetricsChecker(metricDefinition, metrics, variables, elEvaluator);
    metricsChecker.recordMetrics(TestUtil.createSnapshot("testMeterDisabled"));
    Meter meter = MetricsConfigurator.getMeter(metrics, metricDefinition.getId());
    Assert.assertNull(meter);
  }

  @Test
  public void testMeterInvalid() {

    //There is no /age field in record.
    MetricDefinition metricDefinition = new MetricDefinition("testMeterInvalid", "testMeterInvalid", "testMeterInvalid",
      "${record:value(\"/age\")==\"streamsets\"}", "blah", MetricType.METER, true);
    MetricsChecker metricsChecker = new MetricsChecker(metricDefinition, metrics, variables, elEvaluator);
    metricsChecker.recordMetrics(TestUtil.createSnapshot("testMeterInvalid"));
    Meter meter = MetricsConfigurator.getMeter(metrics, metricDefinition.getId());
    Assert.assertNull(meter);
  }

  @Test
  public void testHistogram() {
    MetricDefinition metricDefinition = new MetricDefinition("testMeter", "testMeter", "testHistogram",
      "${record:value(\"/name\")==null}", "blah", MetricType.HISTOGRAM, true);
    MetricsChecker metricsChecker = new MetricsChecker(metricDefinition, metrics, variables, elEvaluator);
    metricsChecker.recordMetrics(TestUtil.createSnapshot("testHistogram"));
    Histogram histogram = MetricsConfigurator.getHistogram(metrics, metricDefinition.getId());
    Assert.assertNotNull(histogram);
    Assert.assertEquals(3, histogram.getCount());
  }

  @Test
  public void testHistogramInvalid() {

    //There is no /age field in record.
    MetricDefinition metricDefinition = new MetricDefinition("testMeterInvalid", "testMeterInvalid", "testHistogramInvalid",
      "${record:value(\"/age\")==\"streamsets\"}", "blah", MetricType.HISTOGRAM, true);
    MetricsChecker metricsChecker = new MetricsChecker(metricDefinition, metrics, variables, elEvaluator);
    metricsChecker.recordMetrics(TestUtil.createSnapshot("testHistogramInvalid"));
    Histogram histogram = MetricsConfigurator.getHistogram(metrics, metricDefinition.getId());
    Assert.assertNull(histogram);
  }
}
