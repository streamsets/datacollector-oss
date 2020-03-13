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
package com.streamsets.datacollector.pipeline.executor.spark;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.pipeline.executor.spark.yarn.YarnAppLauncher;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseExecutor;
import com.streamsets.pipeline.lib.event.EventCreator;
import org.apache.spark.launcher.SparkAppHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_00;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_06;

public class SparkExecutor extends BaseExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(SparkExecutor.class);
  public static final String APP_SUBMITTED_EVENT = "AppSubmittedEvent";
  public static final String APP_ID = "app-id";
  public static final String SUBMITTER = "submitter";
  public static final String TIMESTAMP = "timestamp";

  private final SparkExecutorConfigBean configBean;
  private AppLauncher appLauncher;

  private static final String DEFAULT_USER = "default user (sdc)";
  private String user;

  /**
   * Issued for every submitted Spark job.
   */
  public static final EventCreator JOB_CREATED = new EventCreator.Builder(APP_SUBMITTED_EVENT, 2)
      .withOptionalField(APP_ID)
      .withRequiredField(SUBMITTER)
      .withRequiredField(TIMESTAMP)
      .build();

  public SparkExecutor(SparkExecutorConfigBean conf) {
    this.configBean = conf;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    appLauncher = getLauncher();
    Optional.ofNullable(appLauncher.init(getContext(), configBean)).ifPresent(issues::addAll);

    user = configBean.yarnConfigBean.proxyUser.isEmpty() ? DEFAULT_USER : configBean.yarnConfigBean.proxyUser;

    return issues;
  }

  @Override
  public void destroy() {
    appLauncher.close();
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> records = batch.getRecords();
    SparkAppHandle handle;
    while (records.hasNext()) {
      Record record = records.next();
      try {
        handle = appLauncher.launchApp(record);

        SparkAppHandle.State currentState = handle.getState();
        long currentTime = System.currentTimeMillis();
        long waitforSubmissionUntil = currentTime + (configBean.yarnConfigBean.submitTimeout * 1000);

        // Wait for submitTimeout interval to check if job moves past CONNECTED state onto SUBMITTED state
        while ((currentState == SparkAppHandle.State.UNKNOWN || currentState == SparkAppHandle.State.CONNECTED)
            && currentTime < waitforSubmissionUntil) {
          Thread.sleep(200);
          currentState = handle.getState();
          currentTime = System.currentTimeMillis();
        }
        LOG.debug("Spark application state is '{}'", handle.getState());

        EventCreator.EventBuilder event =
            JOB_CREATED.create(getContext()).with(SUBMITTER, user).with(TIMESTAMP, System.currentTimeMillis()
            );
        // Check if you have an APP ID or not
        // There is no guarantee that the returned appId will always have a value, even after Spark app has completed
        // Check for appId and add it to the event attributes
        String appId = handle.getAppId();
        if (appId != null) {
          LOG.debug("Spark application was launched with app id: '{}'", appId);
          event.with(APP_ID, appId);
        } else {
          LOG.debug("Not able to retrieve app id for Spark application:");
          event.with(APP_ID, (String) null);
        }

        if (!appLauncher.waitForCompletion()) {
          LOG.info("Spark app has been submitted, but it has not completed yet");
        }

        event.createAndSend();

      } catch (ApplicationLaunchFailureException ex) {
        throw new StageException(SPARK_EXEC_00, ex.getMessage(), ex);
      } catch (InterruptedException ex) { //NOSONAR
        LOG.error(SPARK_EXEC_06.getMessage(), ex);
        throw new StageException(SPARK_EXEC_06, ex.getMessage(), ex);
      }
    }
  }

  @VisibleForTesting
  protected AppLauncher getLauncher() {
    return new YarnAppLauncher();
  }

}
