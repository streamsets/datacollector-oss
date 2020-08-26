/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.lib.kinesis;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

@Ignore
public abstract class KinesisCommonUpgraderTest {

    protected StageUpgrader upgrader;
    protected List<Config> configs;
    protected StageUpgrader.Context context;

    @Before
    public void setUp() {
        URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/" + getYamlResourceName());
        upgrader = new SelectorStageUpgrader("stage", null, yamlResource);
        configs = new ArrayList<>();
        context = Mockito.mock(StageUpgrader.Context.class);
    }

    protected abstract String getYamlResourceName();

    @Test
    public void testConnectionIntroductionUpgrade() throws StageException {
        configs.add(new Config(getPrefix() + "awsConfig.credentialMode", "v0"));
        configs.add(new Config(getPrefix() + "awsConfig.awsAccessKeyId", "v1"));
        configs.add(new Config(getPrefix() + "awsConfig.awsSecretAccessKey", "v2"));
        configs.add(new Config(getPrefix() + "region", "v3"));
        configs.add(new Config(getPrefix() + "endpoint", "v4"));
        configs.add(new Config(getPrefix() + "proxyConfig.connectionTimeout", "v5"));
        configs.add(new Config(getPrefix() + "proxyConfig.socketTimeout", "v6"));
        configs.add(new Config(getPrefix() + "proxyConfig.retryCount", "v7"));
        configs.add(new Config(getPrefix() + "proxyConfig.useProxy", "v8"));
        configs.add(new Config(getPrefix() + "proxyConfig.proxyHost", "v9"));
        configs.add(new Config(getPrefix() + "proxyConfig.proxyPort", "v10"));
        configs.add(new Config(getPrefix() + "proxyConfig.proxyUser", "v11"));
        configs.add(new Config(getPrefix() + "proxyConfig.proxyPassword", "v12"));
        configs.add(new Config(getPrefix() + "proxyConfig.proxyDomain", "v13"));
        configs.add(new Config(getPrefix() + "proxyConfig.proxyWorkstation", "v14"));

        Mockito.doReturn(getConnectionIntroductionUpgradeVersion() - 1).when(context).getFromVersion();
        Mockito.doReturn(getConnectionIntroductionUpgradeVersion()).when(context).getToVersion();

        configs = upgrader.upgrade(configs, context);
        UpgraderTestUtils.assertExists(configs, getPrefix() + "connection.awsConfig.credentialMode", "v0");
        UpgraderTestUtils.assertExists(configs, getPrefix() + "connection.awsConfig.awsAccessKeyId", "v1");
        UpgraderTestUtils.assertExists(configs, getPrefix() + "connection.awsConfig.awsSecretAccessKey", "v2");
        UpgraderTestUtils.assertExists(configs, getPrefix() + "connection.region", "v3");
        UpgraderTestUtils.assertExists(configs, getPrefix() + "connection.endpoint", "v4");
        UpgraderTestUtils.assertExists(configs, getPrefix() + "connection.proxyConfig.connectionTimeout", "v5");
        UpgraderTestUtils.assertExists(configs, getPrefix() + "connection.proxyConfig.socketTimeout", "v6");
        UpgraderTestUtils.assertExists(configs, getPrefix() + "connection.proxyConfig.retryCount", "v7");
        UpgraderTestUtils.assertExists(configs, getPrefix() + "connection.proxyConfig.useProxy", "v8");
        UpgraderTestUtils.assertExists(configs, getPrefix() + "connection.proxyConfig.proxyHost", "v9");
        UpgraderTestUtils.assertExists(configs, getPrefix() + "connection.proxyConfig.proxyPort", "v10");
        UpgraderTestUtils.assertExists(configs, getPrefix() + "connection.proxyConfig.proxyUser", "v11");
        UpgraderTestUtils.assertExists(configs, getPrefix() + "connection.proxyConfig.proxyPassword", "v12");
        UpgraderTestUtils.assertExists(configs, getPrefix() + "connection.proxyConfig.proxyDomain", "v13");
        UpgraderTestUtils.assertExists(configs, getPrefix() + "connection.proxyConfig.proxyWorkstation", "v14");
        Assert.assertEquals(15, configs.size());
    }

    protected abstract int getConnectionIntroductionUpgradeVersion();

    protected abstract String getPrefix();

}
