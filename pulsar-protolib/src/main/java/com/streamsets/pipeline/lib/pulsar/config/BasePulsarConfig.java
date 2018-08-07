/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.lib.pulsar.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.origin.pulsar.PulsarSourceConfig;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class BasePulsarConfig {
  private static final Logger LOG = LoggerFactory.getLogger(BasePulsarConfig.class);

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      label = "Pulsar URL",
      description = "Pulsar service URL. Example: http://localhost:8080 or pulsar://localhost:6650",
      displayPosition = 10,
      defaultValue = "http://localhost:8080",
      group = "PULSAR")
  public String serviceURL;

  @ConfigDef(required = false,
      type = ConfigDef.Type.NUMBER,
      label = "Pulsar Keep Alive Interval",
      description = "How often to check whether the connections are still alive. Put time in milliseconds",
      displayPosition = 30,
      defaultValue = "30000",
      min = 0,
      max = 60000,
      group = "PULSAR")
  public int keepAliveInterval;

  @ConfigDef(required = false,
      type = ConfigDef.Type.NUMBER,
      label = "Pulsar Operation Timeout",
      description = "Pulsar Producer-create, Consumer-subscribe and Consumer-unsubscribe operations will be retried " +
          "until this interval, after which the operation will be marked as failed",
      displayPosition = 40,
      defaultValue = "30000",
      min = 0,
      max = 60000,
      group = "PULSAR")
  public int operationTimeout;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Pulsar Configuration Properties",
      displayPosition = 999,
      group = "ADVANCED"
  )
  public Map<String, String> properties = new HashMap<>();

  @ConfigDefBean
  public PulsarSecurityConfig securityConfig;

  private PulsarClient client;

  public List<ConfigIssue> init(Stage.Context context) {
    List<ConfigIssue> issues = new ArrayList<>();

    //Validate BasePulsarConfig configs (currently no validation needed as constraints defined in annotations).

    issues.addAll(extraInit(context));

    issues.addAll(securityConfig.init(context));

    //configure client builder if issues is empty
    if (issues.isEmpty()) {
      ClientBuilder clientBuilder;
      clientBuilder = PulsarClient.builder();
      clientBuilder.serviceUrl(serviceURL)
                   .keepAliveInterval(keepAliveInterval, TimeUnit.MILLISECONDS)
                   .operationTimeout(operationTimeout, TimeUnit.MILLISECONDS);

      // chance to subclass to further configure
      issues = extraBuilderConfiguration(clientBuilder);

      if (issues.isEmpty()) {
        try {
          securityConfig.configurePulsarBuilder(clientBuilder);
        } catch (StageException e) {
          LOG.error(e.toString());
          issues.add(context.createConfigIssue(PulsarGroups.PULSAR.name(), null, e.getErrorCode(), e.getParams()));
        }
        try {
          client = clientBuilder.build();
        } catch (Exception ex) {
          LOG.info(Utils.format(PulsarErrors.PULSAR_00.getMessage(), serviceURL), ex);
          issues.add(context.createConfigIssue(PulsarGroups.PULSAR.name(),
              "pulsarConfig.serviceURL",
              PulsarErrors.PULSAR_00,
              serviceURL,
              ex.toString()
          ));
        }
      }
    }

    return issues;
  }

  public void destroy() {
    if (client != null) {
      try {
        client.close();
      } catch (Exception ex) {
        LOG.warn("Cloud not close Pulsar client: {}", ex);
      }
    }
  }

  /**
   * Extra init tasks performed in this method so a subclass can override this method to add these additional tasks that
   * may be required by the subclass.
   *
   * @param context The context of the stage
   * @return A list of configuration issues found when performing extra init tasks
   */
  protected List<ConfigIssue> extraInit(Stage.Context context) {
    return new ArrayList<>();
  }

  /**
   * extra builder tasks performed in this method so a subclass can override this method to add these additional
   * builder configuration tasks that may be required by the subclass.
   *
   * @param builder The PulsarClient builder
   * @return A list of configuration issues found when performing extra builder configuration tasks
   */
  protected List<ConfigIssue> extraBuilderConfiguration(ClientBuilder builder) {
    return new ArrayList<>();
  }

  public PulsarClient getClient() {
    return client;
  }

}
