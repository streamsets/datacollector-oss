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
package com.streamsets.pipeline.stage.origin.remote;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.lib.remote.Authentication;
import com.streamsets.pipeline.lib.remote.PrivateKeyProvider;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestRemoteDownloadSourceUpgrader {

  @Test
  public void testUpgradeV1ToV2() throws Exception {
    List<Config> configs = new ArrayList<>(1);
    // This should be removed.
    configs.add(new Config("conf.pollInterval", 1000));

    configs = new RemoteDownloadSourceUpgrader().upgrade("a", "b", "v", 1, 2, configs);
    Assert.assertTrue(configs.isEmpty());
  }

  @Test
  public void testUpgradeV3ToV4() throws StageException {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("conf.remoteAddress", "sftp://localhost:1234"));
    configs.add(new Config("conf.auth", Authentication.PASSWORD.name()));
    configs.add(new Config("conf.username", "user"));
    configs.add(new Config("conf.password", "pass"));
    configs.add(new Config("conf.privateKeyProvider", PrivateKeyProvider.FILE.name()));
    configs.add(new Config("conf.privateKey", "/my/private/key.txt"));
    configs.add(new Config("conf.privateKeyPlainText", "MY_PRIVATE_KEY"));
    configs.add(new Config("conf.privateKeyPassphrase", "secret"));
    configs.add(new Config("conf.userDirIsRoot", "true"));
    configs.add(new Config("conf.strictHostChecking", "true"));
    configs.add(new Config("conf.knownHosts", "/hosts"));
    configs.add(new Config("conf.leaveMeAlone", "foo"));

    UpgraderTestUtils.UpgradeMoveWatcher watcher = UpgraderTestUtils.snapshot(configs);

    new RemoteDownloadSourceUpgrader().upgrade("a", "b", "v", 3, 4, configs);

    Assert.assertEquals(12, configs.size());

    watcher.assertAllMoved(
        configs,
        "conf.remoteAddress",
        "conf.remoteConfig.remoteAddress",
        "conf.auth",
        "conf.remoteConfig.auth",
        "conf.username",
        "conf.remoteConfig.username",
        "conf.password",
        "conf.remoteConfig.password",
        "conf.privateKeyProvider",
        "conf.remoteConfig.privateKeyProvider",
        "conf.privateKey",
        "conf.remoteConfig.privateKey",
        "conf.privateKeyPlainText",
        "conf.remoteConfig.privateKeyPlainText",
        "conf.privateKeyPassphrase",
        "conf.remoteConfig.privateKeyPassphrase",
        "conf.userDirIsRoot",
        "conf.remoteConfig.userDirIsRoot",
        "conf.strictHostChecking",
        "conf.remoteConfig.strictHostChecking",
        "conf.knownHosts",
        "conf.remoteConfig.knownHosts"
    );

    UpgraderTestUtils.assertExists(configs, "conf.leaveMeAlone", "foo");
  }
}
