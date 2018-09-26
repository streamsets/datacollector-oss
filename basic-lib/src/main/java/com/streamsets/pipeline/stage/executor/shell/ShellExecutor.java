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
package com.streamsets.pipeline.stage.executor.shell;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseExecutor;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.executor.shell.config.ImpersonationMode;
import com.streamsets.pipeline.stage.executor.shell.config.ShellConfig;
import com.streamsets.pipeline.stage.executor.shell.config.ShellConfigConstants;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class ShellExecutor extends BaseExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(ShellExecutor.class);

  private ShellConfig config;
  private ELEval eval;
  private ErrorRecordHandler errorRecordHandler;
  private ImpersonationMode impersonationMode;
  private String user;
  private String shell;
  private String sudo;
  private long timeout;

  private static final String UNIX_PROCESS_CLASS_NAME = "java.lang.UNIXProcess";
  private static final String PID_FIELD_NAME = "pid";
  private static final int UNDETERMINED_PID = -1;
  private static final Class unixProcessClass;
  private static final Field pidField;
  static {
    // Reflection facility to retrieve process id (we do not depend on this behavior, it's only for troubleshooting)
    Class processClass = null;
    try {
      processClass = Class.forName(UNIX_PROCESS_CLASS_NAME);
    } catch (ClassNotFoundException e) {
      LOG.warn("JVM does not contain class {}", UNIX_PROCESS_CLASS_NAME);
    }

    Field field = null;
    if(processClass != null) {
      try {
        field = processClass.getDeclaredField(PID_FIELD_NAME);
        field.setAccessible(true);
      } catch (NoSuchFieldException e) {
        LOG.warn("Class {} does not contain field {}", UNIX_PROCESS_CLASS_NAME, PID_FIELD_NAME);
      }
    }

    unixProcessClass = processClass;
    pidField = field;
  }

  public ShellExecutor(ShellConfig config) {
    this.config = config;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    this.errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    this.eval = getContext().createELEval("environmentVariables");

    this.impersonationMode = ImpersonationMode.valueOf(
      getContext().getConfiguration().get(ShellConfigConstants.IMPERSONATION_MODE, ShellConfigConstants.IMPERSONATION_MODE_DEFAULT).toUpperCase()
    );

    switch (impersonationMode) {
      case CURRENT_USER:
        user = getContext().getUserContext().getAliasName();
        break;
      case DISABLED:
        break;
      default:
       throw new IllegalArgumentException("Unknown impersonation mode: " + impersonationMode);
    }

    this.shell = getContext().getConfiguration().get(
      ShellConfigConstants.SHELL,
      ShellConfigConstants.SHELL_DEFAULT
    );

    this.sudo = getContext().getConfiguration().get(
      ShellConfigConstants.SUDO,
      ShellConfigConstants.SUDO_DEFAULT
    );

    // We're using static UTC calendar as the timeout is just few seconds/minutes max
    ELVars vars = getContext().createELVars();
    TimeEL.setCalendarInContext(vars, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
    try {
      timeout = getContext().createELEval("timeout").eval(vars, config.timeout, Long.class);

      if(timeout < 0) {
        issues.add(getContext().createConfigIssue("ENVIRONMENT", "timeout", Errors.SHELL_005, timeout));
      }
    } catch (ELEvalException e) {
      issues.add(getContext().createConfigIssue("ENVIRONMENT", "timeout", Errors.SHELL_004, e.getMessage(), e));
    }

    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();
      executeScript(record);
    }
  }

  private void executeScript(Record record) throws StageException {
    File script = null;
    try {
      script = File.createTempFile("sdc-script-executor", ".sh");
      ELVars variables = getContext().createELVars();
      RecordEL.setRecordInContext(variables, record);

      // Serialize the script into a file on disk (in temporary location)
      FileUtils.writeStringToFile(script, config.script);

      ImmutableList.Builder<String> commandBuilder = new ImmutableList.Builder<>();
      if(impersonationMode != ImpersonationMode.DISABLED) {
        commandBuilder.add(sudo);
        commandBuilder.add("-E");
        commandBuilder.add("-u");
        commandBuilder.add(user);
      }

      commandBuilder.add(shell);
      commandBuilder.add(script.getPath());

      List<String> commandLine = commandBuilder.build();

      // External process configuration
      ProcessBuilder processBuilder = new ProcessBuilder(commandLine);
      for(Map.Entry<String, String> entry : config.environmentVariables.entrySet()) {
        processBuilder.environment().put(
          eval.eval(variables, entry.getKey(), String.class),
          eval.eval(variables, entry.getValue(), String.class)
        );
      }

      // Start process and configure forwarders for stderr/stdin
      LOG.debug("Executing script: {}", StringUtils.join(commandLine, " "));
      Process process = processBuilder.start();
      new Thread(new ProcessStdIOForwarder(false, process.getInputStream())).start();
      new Thread(new ProcessStdIOForwarder(true, process.getErrorStream())).start();

      int pid = retrievePidIfFeasible(process);
      LOG.debug("Created process with PID {}", pid);

      // User configures the maximal time for the script execution
      boolean finished = process.waitFor(timeout, TimeUnit.MILLISECONDS);
      if(!finished) {
        process.destroyForcibly();
        throw new OnRecordErrorException(record, Errors.SHELL_002);
      }

      if(process.exitValue() != 0) {
        throw new OnRecordErrorException(record, Errors.SHELL_003, process.exitValue());
      }
    } catch(OnRecordErrorException e) {
      errorRecordHandler.onError(e);
    } catch (Exception e) {
      errorRecordHandler.onError(new OnRecordErrorException(record, Errors.SHELL_001, e.toString(), e));
    } finally {
      if(script != null && script.exists()) {
        script.delete();
      }
    }
  }

  /**
   * Attempts to retrieve PID from internal JVM classes. This method is not guaranteed to work as JVM is free
   * to change their implementation at will. Hence the return value should be only used for troubleshooting or
   * debug and not for main functionality.
   */
  private static int retrievePidIfFeasible(Process process) {
    if(unixProcessClass == null) {
      return UNDETERMINED_PID;
    }

    if(!unixProcessClass.isInstance(process)) {
      LOG.debug("Do not support retrieving PID from {}", process.getClass().getName());
      return UNDETERMINED_PID;
    }

    try {
      return (int)pidField.get(process);
    } catch (IllegalAccessException e) {
      LOG.debug("Can't retrieve PID value from the field", e);
      return UNDETERMINED_PID;
    }

  }

  /**
   * Forwarder of stdout and stderr to our logs, so that we can easily search the script output in our logs (by
   * pipeline and runner id).
   */
  private static class ProcessStdIOForwarder implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessStdIOForwarder.class);

    private final boolean error;
    private final InputStream inputStream;

    public ProcessStdIOForwarder(boolean error, InputStream inputStream) {
      this.error = error;
      this.inputStream = inputStream;
    }

    @Override
    public void run() {
      Thread.currentThread().setName("Shell Executor IO Forwarder thread " + (error ? "stderr" : "stdout"));

      try {
        InputStreamReader reader = new InputStreamReader(inputStream);
        Scanner scan = new Scanner(reader);
        while (scan.hasNextLine()) {
          if (error) {
            LOG.error("stderr: " + scan.nextLine());
          } else {
            LOG.info("stdout: " + scan.nextLine());
          }
        }
      } finally {
        IOUtils.closeQuietly(inputStream);
      }
    }
  }
}
