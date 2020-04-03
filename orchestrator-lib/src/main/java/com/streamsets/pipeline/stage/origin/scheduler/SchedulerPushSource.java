/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.scheduler;

import com.streamsets.pipeline.api.PushSource;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class SchedulerPushSource implements PushSource {

  private static final Logger LOG = LoggerFactory.getLogger(SchedulerPushSource.class);
  final static String PUSH_SOURCE_CONTEXT = "pushSourceContext";
  private final SchedulerConfig conf;
  private Context context;
  private Scheduler scheduler;
  private CronTrigger cronTrigger;
  private BlockingQueue<Exception> errorQueue;
  private List<Exception> errorList;

  SchedulerPushSource(SchedulerConfig conf) {
    this.conf = conf;
  }

  @Override
  public int getNumberOfThreads() {
    return 1;
  }

  @Override
  public List<ConfigIssue> init(Info info, Context context) {
    List<ConfigIssue> issues = new ArrayList<>();
    this.context = context;
    errorQueue = new ArrayBlockingQueue<>(100);
    errorList = new ArrayList<>(100);
    try {
      // Default values
      Properties properties = new Properties();
      properties.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME, context.getPipelineId());
      properties.setProperty(StdSchedulerFactory.PROP_SCHED_RMI_EXPORT, "false");
      properties.setProperty(StdSchedulerFactory.PROP_SCHED_RMI_PROXY, "false");
      properties.setProperty(StdSchedulerFactory.PROP_SCHED_WRAP_JOB_IN_USER_TX, "false");
      properties.setProperty(StdSchedulerFactory.PROP_THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
      properties.setProperty("org.quartz.threadPool.threadCount", "10");
      properties.setProperty("org.quartz.threadPool.threadPriority", "5");
      properties.setProperty("org.quartz.threadPool.threadsInheritContextClassLoaderOfInitializingThread", "true");
      properties.setProperty("org.quartz.jobStore.misfireThreshold", "60000");
      properties.setProperty("org.quartz.jobStore.class", "org.quartz.simpl.RAMJobStore");

      SchedulerFactory schedulerFactory = new StdSchedulerFactory(properties);
      scheduler = schedulerFactory.getScheduler();
    } catch (Exception ex) {
      LOG.error(ex.toString(), ex);
      issues.add(
          context.createConfigIssue(
              Groups.CRON.getLabel(),
              "conf.cronExpression",
              Errors.SCHEDULER_01,
              ex.getMessage(),
              ex
          )
      );
    }
    return issues;
  }

  @Override
  public void produce(Map<String, String> lastOffsets, int maxBatchSize) {
    try {
      JobDetail job = JobBuilder.newJob(SchedulerJob.class)
          .withIdentity(context.getPipelineId(), "dataCollectorJobGroup")
          .build();
      CronScheduleBuilder cronScheduleBuilder = CronScheduleBuilder.cronSchedule(conf.cronExpression)
          .inTimeZone(TimeZone.getTimeZone(ZoneId.of(conf.timeZoneID)));
      cronTrigger = TriggerBuilder.newTrigger()
          .withIdentity(context.getPipelineId(), "dataCollectorJobGroup")
          .withSchedule(cronScheduleBuilder)
          .build();

      scheduler.getContext().put(PUSH_SOURCE_CONTEXT, context);
      scheduler.scheduleJob(job, cronTrigger);

      scheduler.start();

      while (!context.isStopped()) {
        dispatchErrors();
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      context.reportError(e);
    }
  }

  private void dispatchErrors() {
    try {
      Thread.sleep(100);
    } catch (InterruptedException ignored) { }
    errorList.clear();
    errorQueue.drainTo(errorList);
    for (Exception exception : errorList) {
      context.reportError(exception);
    }
  }

  @Override
  public void destroy() {
    if (scheduler != null) {
      try {
        if (cronTrigger != null) {
          scheduler.unscheduleJob(cronTrigger.getKey());
        }
        scheduler.shutdown(true);
      } catch (SchedulerException e) {
        LOG.error(e.getMessage(), e);
        context.reportError(e);
      }
    }
  }
}
