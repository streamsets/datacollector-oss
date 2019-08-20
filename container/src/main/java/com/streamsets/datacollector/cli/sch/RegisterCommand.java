/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.cli.sch;

import com.streamsets.datacollector.restapi.bean.DPMInfoJson;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

@Command(name = "register", description = "Register this Data Collector in Control Hub.")
public class RegisterCommand extends AbstractCommand {
  private static final Logger logger = LoggerFactory.getLogger(RegisterCommand.class);

  @Option(
    name = {"-l", "--url"},
    description = "Control Hub URL",
    required = true
  )
  private String baseURL;

  @Option(
    name = {"--labels"},
    description = "Comma separate list of labels that the Data Collector should be registered with"
  )
  private String labels;

  @Override
  protected void executeAction() throws Exception {
    DPMInfoJson infoJson = new DPMInfoJson();
    infoJson.setBaseURL(baseURL);
    infoJson.setUserID(getUserID());
    infoJson.setUserPassword(getUserPassword());
    infoJson.setOrganization(getOrganization());
    infoJson.setLabels(Collections.emptyList());

    if(labels != null) {
      infoJson.setLabels(Arrays.asList(labels.split(",")));
    }

    SchAdmin.enableDPM(
      infoJson,
      new SchAdmin.Context(getRuntimeInfo(), getConfiguration(), isSkipConfigUpdate(), getTokenFilePath())
    );

    logger.info("SCH registration successful. Please restart {} to complete.", getRuntimeInfo().getProductName());
  }
}
