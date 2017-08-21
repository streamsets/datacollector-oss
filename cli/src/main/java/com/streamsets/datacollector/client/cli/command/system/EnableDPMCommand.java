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
import com.streamsets.datacollector.client.model.DPMInfoJson;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import java.util.Arrays;

@Command(name = "enableDPM", description = "Enable DPM")
public class EnableDPMCommand extends BaseCommand {

  @Option(
      name = {"--dpmUrl"},
      description = "DPM URL",
      required = true
  )
  public String dpmBaseURL;

  @Option(
      name = {"--dpmUser"},
      description = "DPM User ID",
      required = true
  )
  public String dpmUserID;

  @Option(
      name = {"--dpmPassword"},
      description = "DPM password",
      required = true
  )
  public String dpmUserPassword;

  @Option(
      name = {"--labels"},
      description = "Labels to report the remote control system",
      required = false
  )
  public String labels;

  @Override
  public void run() {
    try {
      SystemApi systemApi = new SystemApi(getApiClient());
      String organization = getOrganizationId();
      if (organization == null) {
        System.out.println("DPM User ID must be <ID>@<Organization ID>");
        return;
      }

      DPMInfoJson dpmInfo = new DPMInfoJson();
      dpmInfo.setBaseURL(dpmBaseURL);
      dpmInfo.setUserID(dpmUserID);
      dpmInfo.setUserPassword(dpmUserPassword);
      dpmInfo.setOrganization(organization);
      if (labels != null && labels.trim().length() > 0) {
        dpmInfo.setLabels(Arrays.asList(labels.split(",")));
      }

      systemApi.enableDPM(dpmInfo);
      System.out.println("Successfully generated application token and updated the file 'etc/application-token.txt' " +
          "and 'etc/dpm.properties'. Restart the Data Collector to register Application Token.");
    } catch (Exception ex) {
      if(printStackTrace) {
        ex.printStackTrace();
      } else {
        System.out.println(ex.getMessage());
      }
    }
  }

  private String getOrganizationId() {
    if (dpmUserID != null) {
      String[] parts = dpmUserID.split("@");
      if( parts.length == 2) {
        return parts[1];
      }
    }
    return null;
  }
}
