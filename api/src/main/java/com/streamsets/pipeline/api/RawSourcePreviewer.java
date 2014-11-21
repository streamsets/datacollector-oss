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

import java.io.Reader;
import java.util.Map;

public interface RawSourcePreviewer {

  //TODO replace with config def properties
  Map<String, String> getParameters();

  //Review looks up config values from
  Reader preview(Map<String, String> previewParams, int maxLength);

  String getMime();

  //TODO IMplementation of this interface will have properties with COnfigDef annotation specified on them
  //Generated stage def json will capture it
  //At run time there will ConfigConfiguration objects [name, value ] and that must be set on the instance of
  //the implementation, also take care of localization.
}
