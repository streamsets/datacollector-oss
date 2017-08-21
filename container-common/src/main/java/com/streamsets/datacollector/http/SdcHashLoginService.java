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
package com.streamsets.datacollector.http;

import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.resource.Resource;

import java.io.IOException;

public class SdcHashLoginService extends HashLoginService {

  SdcHashLoginService(String name, String config) {
    super(name, config);
  }

  public UserIdentity getUserIdentity(String name) {
    return _propertyUserStore.getUserIdentity(name);
  }

  public Resource getResolvedConfigResource() throws IOException {
    return _propertyUserStore.getConfigResource();
  }

}
