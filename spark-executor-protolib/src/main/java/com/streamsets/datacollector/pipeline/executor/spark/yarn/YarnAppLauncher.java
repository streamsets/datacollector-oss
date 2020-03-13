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
package com.streamsets.datacollector.pipeline.executor.spark.yarn;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.pipeline.executor.spark.AppLauncher;
import com.streamsets.datacollector.pipeline.executor.spark.DeployMode;
import com.streamsets.datacollector.pipeline.executor.spark.Errors;
import com.streamsets.datacollector.pipeline.executor.spark.SparkExecutorConfigBean;
import com.streamsets.datacollector.pipeline.executor.spark.ApplicationLaunchFailureException;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.el.ELEvalException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_01;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_02;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_03;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_04;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_05;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_07;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_08;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_09;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_10;

public class YarnAppLauncher implements AppLauncher {

  private static final String YARN = "yarn";
  private static final String PREFIX = "conf.yarnConfigBean.";
  private static final String JAVA_HOME_CONF = "conf.javaHome";
  private static final String SPARK_HOME_CONF = "conf.sparkHome";
  private static final String MEMORY_STRING_REGEX = "[0-9]+[gGmMkK]$";
  private static final String SPARK_GROUP = "SPARK";
  private static final String APPLICATION_GROUP = "APPLICATION";
  private CountDownLatch latch = new CountDownLatch(1);
  private long timeout;
  private SparkExecutorConfigBean configs;
  private YarnConfigBean yarnConfigs;

  @Override
  public List<Stage.ConfigIssue> init(Stage.Context context, SparkExecutorConfigBean configs) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    this.configs = configs;
    this.yarnConfigs = configs.yarnConfigBean;

    Optional.ofNullable(yarnConfigs.init(context, PREFIX)).ifPresent(issues::addAll);

    verifyKeyValueArgs(context, yarnConfigs.env, PREFIX + "env", issues);

    verifyKeyValueArgs(context, yarnConfigs.args, PREFIX + "args", issues);

    verifyJavaAndSparkHome(context, issues);

    verifyMemoryStrings(context, issues);

    verifyAllFileProperties(context, issues);

    if(!StringUtils.isEmpty(configs.credentialsConfigBean.principal)) {
      if (!StringUtils.isEmpty(configs.credentialsConfigBean.keytab)) {
        verifyFileIsAccessible(
            configs.credentialsConfigBean.keytab, context, issues, SPARK_GROUP, PREFIX + "keytab");
      } else {
        issues.add(context.createConfigIssue(SPARK_GROUP, PREFIX + "keytab", SPARK_EXEC_05));
      }
    }

    return issues;
  }

  private void verifyKeyValueArgs(
      Stage.Context context,
      Map<String, String> params,
      String config,
      List<Stage.ConfigIssue> issues
  ) {
    params.forEach((String k, String v) -> {
      if (!StringUtils.isEmpty(k) && StringUtils.isEmpty(v)) {
        issues.add(context.createConfigIssue(SPARK_GROUP, config, SPARK_EXEC_09, k));
      }
      if (StringUtils.isEmpty(k) && !StringUtils.isEmpty(v)) {
        issues.add(context.createConfigIssue(SPARK_GROUP, config, SPARK_EXEC_10, v));
      }
    });
  }

  private void verifyAllFileProperties(
      Stage.Context context,
      List<Stage.ConfigIssue> issues
  ) {

    verifyFileIsAccessible(
        yarnConfigs.appResource, context, issues, APPLICATION_GROUP, PREFIX + "appResource");

    verifyFilesAreAccessible(
        yarnConfigs.additionalJars, context, issues, APPLICATION_GROUP, PREFIX + "additionalJars");

    verifyFilesAreAccessible(
        yarnConfigs.additionalFiles, context, issues, APPLICATION_GROUP, PREFIX + "additionalFiles");

    verifyFilesAreAccessible(yarnConfigs.pyFiles, context, issues, APPLICATION_GROUP, PREFIX + "pyFiles");
  }

  private void verifyMemoryStrings(
      Stage.Context context,
      List<Stage.ConfigIssue> issues
  ) {
    if (!isValidMemoryString(yarnConfigs.driverMemory)) {
      issues.add(context.createConfigIssue(
          SPARK_GROUP, PREFIX + "driverMemory", SPARK_EXEC_03, yarnConfigs.driverMemory));
    }

    if (!isValidMemoryString(yarnConfigs.executorMemory)) {
      issues.add(context.createConfigIssue(
          SPARK_GROUP, PREFIX + "executorMemory", SPARK_EXEC_03, yarnConfigs.executorMemory));
    }
  }

  private void verifyJavaAndSparkHome(
      Stage.Context context,
      List<Stage.ConfigIssue> issues
  ) {

    if (StringUtils.isEmpty(configs.sparkHome)) {
      String sparkHomeEnv = System.getenv("SPARK_HOME");
      if (StringUtils.isEmpty(sparkHomeEnv)) {
        issues.add(context.createConfigIssue(SPARK_GROUP, SPARK_HOME_CONF, SPARK_EXEC_01));
      } else {
        verifyFileIsAccessible(sparkHomeEnv, context, issues, SPARK_GROUP, SPARK_HOME_CONF, SPARK_EXEC_08);
      }
    } else {
      verifyFileIsAccessible(configs.sparkHome, context, issues, SPARK_GROUP, SPARK_HOME_CONF);
    }

    if (StringUtils.isEmpty(configs.javaHome)) {
      String javaHomeEnv = System.getenv("JAVA_HOME");
      if (StringUtils.isEmpty(javaHomeEnv)) {
        issues.add(context.createConfigIssue(SPARK_GROUP, JAVA_HOME_CONF, SPARK_EXEC_02));
      } else {
        verifyFileIsAccessible(javaHomeEnv, context, issues, SPARK_GROUP, JAVA_HOME_CONF, SPARK_EXEC_07);
      }
    } else {
      verifyFileIsAccessible(configs.javaHome, context, issues, SPARK_GROUP, JAVA_HOME_CONF);
    }
  }

  private boolean isValidMemoryString(String memoryString) {
    Optional<Boolean> valid =
        Optional.ofNullable(memoryString).map((String x) -> x.matches(MEMORY_STRING_REGEX));
    return valid.isPresent() && valid.get();
  }

  private void verifyFilesAreAccessible(
      List<String> files,
      Stage.Context context,
      List<Stage.ConfigIssue> issues,
      String configGroup,
      String config
  ) {
    files.forEach((String file) -> verifyFileIsAccessible(file, context, issues, configGroup, config));
  }

  private void verifyFileIsAccessible(
      String file,
      Stage.Context context,
      List<Stage.ConfigIssue> issues,
      String configGroup,
      String config
  ) {
    verifyFileIsAccessible(file, context, issues, configGroup, config, SPARK_EXEC_04);
  }

  private void verifyFileIsAccessible(
      String file,
      Stage.Context context,
      List<Stage.ConfigIssue> issues,
      String configGroup,
      String config,
      Errors error
  ) {
    File f = new File(file);
    if (!f.exists() || !f.canRead()) {
      issues.add(context.createConfigIssue(configGroup, config, error, file));
    }
  }

  @Override
  public SparkAppHandle launchApp(Record record)
      throws ApplicationLaunchFailureException, ELEvalException {

    SparkLauncher launcher = getLauncher();

    if (yarnConfigs.language == Language.JVM) {
      launcher.setMainClass(yarnConfigs.mainClass);
    }

    launcher.setAppResource(yarnConfigs.appResource)
        .setAppName(yarnConfigs.appName)
        .setMaster(YARN)
        .setDeployMode(yarnConfigs.deployMode.getLabel().toLowerCase())
        .setVerbose(yarnConfigs.verbose);

    if (yarnConfigs.dynamicAllocation) {
      launcher.setConf("spark.dynamicAllocation.enabled", "true");
      launcher.setConf("spark.shuffle.service.enabled", "true");
      launcher.setConf("spark.dynamicAllocation.minExecutors", String.valueOf(yarnConfigs.minExecutors));
      launcher.setConf("spark.dynamicAllocation.maxExecutors", String.valueOf(yarnConfigs.maxExecutors));
    } else {
      launcher.setConf("spark.dynamicAllocation.enabled", "false");
      launcher.addSparkArg("--num-executors", String.valueOf(yarnConfigs.numExecutors));
    }

    launcher.addSparkArg("--executor-memory", yarnConfigs.executorMemory);
    launcher.addSparkArg("--driver-memory", yarnConfigs.driverMemory);

    if (yarnConfigs.deployMode == DeployMode.CLUSTER && yarnConfigs.waitForCompletion) {
      launcher.setConf("spark.yarn.submit.waitAppCompletion", "true");
    }

    // Default is empty string, so pass only non-empty ones.
    yarnConfigs.noValueArgs.forEach((String arg) -> applyConfIfPresent(arg, launcher::addSparkArg));
    yarnConfigs.args.forEach((String k, String v) -> applyConfIfPresent(k, v, launcher::addSparkArg));

    // For files, no need of removing empty strings, since we verify the file exists in init itself.
    yarnConfigs.additionalFiles.forEach(launcher::addFile);
    yarnConfigs.additionalJars.forEach(launcher::addJar);
    yarnConfigs.pyFiles.forEach(launcher::addPyFile);

    launcher.addAppArgs(getNonEmptyArgs(yarnConfigs.evaluateArgsELs(record)));

    applyConfIfPresent(configs.javaHome, launcher::setJavaHome);
    applyConfIfPresent(
        "spark.yarn.principal", configs.credentialsConfigBean.principal, launcher::setConf);
    applyConfIfPresent("spark.yarn.keytab", configs.credentialsConfigBean.keytab, launcher::setConf);
    applyConfIfPresent("--proxy-user", yarnConfigs.proxyUser, launcher::addSparkArg);
    applyConfIfPresent(configs.sparkHome, launcher::setSparkHome);

    timeout = yarnConfigs.waitTimeout;

    try {
      final SparkAppHandle handle = launcher.startApplication(new AppListener());
      return handle;
    } catch (IOException ex) {
      latch.countDown();
      throw new ApplicationLaunchFailureException(ex);
    } catch (Throwable ex) { // NOSONAR
      latch.countDown();
      throw ex;
    }

  }

  /**
   * If there is a RecordEL, then an arg could eval to empty string. This method returns
   * args that are not null or empty.
   */
  private String[] getNonEmptyArgs(List<String> appArgs) {
    List<String> nonEmpty = new ArrayList<>();
    appArgs.forEach((String val) -> {
      if (!StringUtils.isEmpty(val)) {
        nonEmpty.add(val);
      }
    });
    return nonEmpty.toArray(new String[nonEmpty.size()]);
  }

  @VisibleForTesting
  protected SparkLauncher getLauncher() {
    return new SparkLauncher(yarnConfigs.env);
  }

  private static void applyConfIfPresent(String config, Consumer<? super String> configFn) {
    // Empty is valid, since the user may have just created a new one by clicking (+), but not
    // entered anything. So just don't pass it along.
    if (!StringUtils.isEmpty(config)) {
      configFn.accept(config);
    }
  }

  @SuppressWarnings("ReturnValueIgnored")
  private static void applyConfIfPresent(
      String configName,
      String configValue,
      BiFunction<String, String, ?> configFn
  ) {
    // Both being empty is valid, since the user may have just created a new one by clicking (+), but not
    // entered anything. So just don't pass it along.
    // Just one being empty is taken care of by the init method.
    if (!StringUtils.isEmpty(configName) && !StringUtils.isEmpty(configValue)) {
      configFn.apply(configName, configValue);
    }
  }

  @Override
  public boolean waitForCompletion() throws InterruptedException {
    if (yarnConfigs.waitForCompletion) {
      if (timeout > 0) {
        return latch.await(timeout, TimeUnit.MILLISECONDS);
      } else {
        latch.await();
        return true;
      }
    }
    return true;
  }

  @Override
  public void close() {
    // No op
  }

  private class AppListener implements SparkAppHandle.Listener {
    @Override
    public void stateChanged(SparkAppHandle handle) {
      if (appCompleted(handle)) {
        latch.countDown();
      }
    }

    @Override
    public void infoChanged(SparkAppHandle handle) { // NOSONAR
    }

    private boolean appCompleted(SparkAppHandle handle) {
      return
          handle.getState() == SparkAppHandle.State.FAILED
              || handle.getState() == SparkAppHandle.State.FINISHED
              || handle.getState() == SparkAppHandle.State.KILLED;
    }
  }

}
