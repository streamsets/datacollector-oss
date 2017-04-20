/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.bundles;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Writer for content generator classes.
 *
 * Subclasses OutputStream by adding adding methods that are relevant to bundles creation (stores whole file, ...)
 */
public abstract class BundleWriter extends OutputStream {

  /**
   * Create new file in the bundle.
   *
   * @param name Path and name of the file that should be started.
   */
  public abstract void markStartOfFile(String name) throws IOException;

  /**
   * Needs to be called after each file is done writing.
   */
  public abstract void markEndOfFile() throws IOException;

  /**
   * Convenience method to write out string with platform default encoding.
   * @param str String to be written out
   */
  public void write(String str) throws IOException {
    if(str == null) {
      return;
    }

    write(str.getBytes());
  }

  /**
   * Write the content of give file to the bundle.
   *
   * The caller must call mark*File() methods before and after this call.
   *
   * @param input Input file to be written into the stream
   */
  public void writeFile(File input) throws IOException {
    try(FileInputStream inputStream = new FileInputStream(input)) {
      IOUtils.copy(inputStream, this);
    }
  }

}
