/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.stage.processor.tee;

import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.lib.jdbc.EncryptionGroups;
import com.streamsets.pipeline.stage.processor.jdbctee.JdbcTeeDProcessor;

//@StageDef(
//    version = 3,
//    label = "PostgreSQL Tee",
//    description = "Write records to PostgreSQL and enrich records with generated columns",
//    upgrader = JdbcTeeUpgrader.class,
//    icon = "postgresql.png",
//    onlineHelpRefUrl = "index.html?contextID=task_o1t_phv_33b"
//)
@ConfigGroups(value = EncryptionGroups.class)
@GenerateResourceBundle
@HideConfigs(value = {
    "hikariConfigBean.readOnly", "hikariConfigBean.autoCommit",
})
public class PostgresTeeDProcessor extends JdbcTeeDProcessor {}
