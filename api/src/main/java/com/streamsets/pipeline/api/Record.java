/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.api;

import java.util.Set;

public interface Record {

  public interface Header {

    public String getStageCreator();

    public String getSourceId();

    public String getTrackingId();

    public String getPreviousStageTrackingId();

    public String getStagesPath();

    public byte[] getRaw();

    public String getRawMimeType();

    public Set<String> getAttributeNames();

    public String getAttribute(String name);

    public void setAttribute(String name, String value);

    public void deleteAttribute(String name);

  }

  public Header getHeader();

  public Field get();

  public Field set(Field field);

  /*
    Field path syntax

     "" : ROOT element
     /a : map element
     [1] : array element
     /a[2] : map array element
     [1]/a : array map element
     [1][2] : array array element
     /a/b : map map element
   */

  public Field get(String fieldPath);

  public Field delete(String fieldPath);

  public boolean has(String fieldPath);

  public Set<String> getFieldPaths();

}
