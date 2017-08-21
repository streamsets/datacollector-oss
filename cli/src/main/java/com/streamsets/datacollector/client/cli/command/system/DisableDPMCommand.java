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
package com.streamsets.datacollector.client.cli.command.system;

import com.streamsets.datacollector.client.api.SystemApi;
import com.streamsets.datacollector.client.cli.command.BaseCommand;
import io.airlift.airline.Command;

@Command(name = "disableDPM", description = "Disable DPM")
public class DisableDPMCommand extends BaseCommand {
  @Override
  public void run() {
    try {
      SystemApi systemApi = new SystemApi(getApiClient());
      systemApi.disableDPM();
      System.out.println("Successfully disabled DPM by updating file'etc/dpm.properties'. " +
          "Restart the Data Collector for the changes to take effect. ");
    } catch (Exception ex) {
      if(printStackTrace) {
        ex.printStackTrace();
      } else {
        System.out.println(ex.getMessage());
      }
    }
  }
}
