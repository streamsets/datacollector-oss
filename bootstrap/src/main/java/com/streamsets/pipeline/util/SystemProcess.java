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
package com.streamsets.pipeline.util;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface SystemProcess {
  public void start() throws IOException;
  public void start(Map<String, String> env) throws IOException;

  public String getCommand();

  public boolean isAlive();

  public void cleanup();

  public Collection<String> getAllOutput();

  public Collection<String> getAllError();

  public Collection<String> getOutput();

  public Collection<String> getError();

  public void kill(long timeoutBeforeForceKill) ;

  public int exitValue();

  public boolean waitFor(long timeout, TimeUnit unit);

}
