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
package com.streamsets.pipeline.stage.executor.shell.config;

public class ShellConfigConstants {

  /**
   * String containing impersonation mode for the shell executor, valid values
   * are in {@ImpersonationMode} enum.
   *
   * Used in sdc.properties as stage config property.
   */
  public static final String IMPERSONATION_MODE = "com.streamsets.pipeline.stage.executor.shell.impersonation_mode";
  public static final String IMPERSONATION_MODE_DEFAULT = ImpersonationMode.DISABLED.name();

  /**
   * Shell that should be used to execute use supplied script. Will be just "sh" by default, another possible values
   * are for example /bin/bash or even better /bin/zsh.
   *
   * Used in sdc.properties as stage config property.
   */
  public static final String SHELL = "com.streamsets.pipeline.stage.executor.shell.shell";
  public static final String SHELL_DEFAULT = "sh";

  /**
   * sudo command that should be executed. Will default only to 'sudo' (expecting it on the path).
   *
   * Used in sdc.properties as stage config property.
   */
  public static final String SUDO = "com.streamsets.pipeline.stage.executor.shell.sudo";
  public static final String SUDO_DEFAULT = "sudo";
}
