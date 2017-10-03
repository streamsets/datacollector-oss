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
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

  public ShellExecutor(ShellConfig config) {
    this.config = config;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    this.errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    this.eval = getContext().createELEval("environmentVariables");

    this.impersonationMode = ImpersonationMode.valueOf(
      Optional.ofNullable(getContext().getConfig(ShellConfigConstants.IMPERSONATION_MODE))
              .orElse(ShellConfigConstants.IMPERSONATION_MODE_DEFAULT)
      .toUpperCase()
    );

    switch (impersonationMode) {
      case CURRENT_USER:
        user = getContext().getUserContext().getUser();
        break;
      case DISABLED:
        break;
      default:
       throw new IllegalArgumentException("Unknown impersonation mode: " + impersonationMode);
    }

    Optional<String> shellConfig = Optional.ofNullable(getContext().getConfig(ShellConfigConstants.SHELL));
    this.shell = shellConfig.orElse(ShellConfigConstants.SHELL_DEFAULT);

    Optional<String> sudoConfig = Optional.ofNullable(getContext().getConfig(ShellConfigConstants.SUDO));
    this.sudo = sudoConfig.orElse(ShellConfigConstants.SUDO_DEFAULT);

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
