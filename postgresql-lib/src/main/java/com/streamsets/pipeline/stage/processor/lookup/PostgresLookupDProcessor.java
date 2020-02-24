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

package com.streamsets.pipeline.stage.processor.lookup;

import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.lib.jdbc.EncryptionGroups;
import com.streamsets.pipeline.stage.processor.jdbclookup.JdbcLookupDProcessor;

//@StageDef(
//    version = 3,
//    label = "PostgreSQL Lookup",
//    description = "Lookup values via PostgreSQL to enrich records.",
//    icon = "postgresql.png",
//    upgrader = JdbcLookupProcessorUpgrader.class,
//    onlineHelpRefUrl = "index.html?contextID=task_z4z_yhv_33b"
//)
@ConfigGroups(value = EncryptionGroups.class)
@GenerateResourceBundle
public class PostgresLookupDProcessor extends JdbcLookupDProcessor {}
