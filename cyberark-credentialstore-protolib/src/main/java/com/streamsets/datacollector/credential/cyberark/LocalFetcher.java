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
package com.streamsets.datacollector.credential.cyberark;

import com.streamsets.pipeline.api.StageException;

import java.util.Map;

/**
 * CyberArk Local Credential Provider implementation, using CyberArk JAVA API.
 */
public class LocalFetcher implements Fetcher {

  @Override
  public void init(Configuration conf) {
    throw new UnsupportedOperationException(); //NOT IMPLEMENTED YET
  }

  @Override
  public String fetch(String group, String name, Map<String, String> options) throws StageException {
    throw new UnsupportedOperationException(); //NOT IMPLEMENTED YET
  }

  @Override
  public void destroy() {
    throw new UnsupportedOperationException(); //NOT IMPLEMENTED YET
  }
}
