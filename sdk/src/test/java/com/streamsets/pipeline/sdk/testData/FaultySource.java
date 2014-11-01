/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.sdk.testData;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldModifier;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.StageDef;

import java.util.List;
import java.util.Map;

/**
 * This class has the following issues:
 * 1. Configuration field is final
 * 2. Configuration field is static
 * 3. Configuration field is not public
 * 4. Configuration field is marked as "MODEL" but not annotated with either FieldSelector or FieldModifier annotation
 * 5. No default constructor
 * 6. Does not implement interface or extend base stage
 * 7. Data type of field does not match type indicated in the config def annotation
 * 8. Configuration field marked with FieldSelector annotation must be of type List<String>
 * 9. Configuration field marked with FieldModifier annotation must be of type Map<String, String>
 * 10. Both FieldSelector and FieldModifier annotations are present
 */
@StageDef(name = "TwitterSource", description = "Produces twitter feeds", label = "twitter_source"
  , version = "1.0")
public class FaultySource {

  //1.Faulty config should not be final
  @ConfigDef(
    name = "username",
    defaultValue = "admin",
    label = "username",
    required = true,
    description = "The user name of the twitter user",
    type = ConfigDef.Type.STRING
  )
  public final String username;

  //2.faulty string, should not be static
  @ConfigDef(
    name = "password",
    defaultValue = "admin",
    label = "password",
    required = true,
    description = "The password the twitter user",
    type = ConfigDef.Type.STRING
  )
  public static String password;

  //3.Faulty field, should be public
  @ConfigDef(
    name = "streetAddress2",
    defaultValue = "",
    label = "streetAddress2",
    required = true,
    description = "The domain of the twitter user",
    type = ConfigDef.Type.STRING
  )
  private String streetAddress2;

  //4. Expected either FieldSelector or FieldModifier annotation
  @ConfigDef(
    name = "company",
    defaultValue = "ss",
    label = "company",
    required = true,
    description = "The domain of the twitter user",
    type = ConfigDef.Type.MODEL
  )
  public String company;

  //5. No default constructor
  public FaultySource(String username, String password) {
    this.username = username;
    this.password = password;
  }

  //6. The class neither implements an interface nor extends from a base class

  //7. The type is expected to be string but is int
  @ConfigDef(
    name = "zip",
    defaultValue = "94040",
    label = "zip",
    required = true,
    description = "The domain of the twitter user",
    type = ConfigDef.Type.STRING)
  public int zip;

  //8. Field selector should be modeled as List<String>
  @FieldSelector
  @ConfigDef(
    name = "state",
    defaultValue = "CA",
    label = "state",
    required = true,
    description = "The domain of the twitter user",
    type = ConfigDef.Type.MODEL)
  public String state;


  //9. Field modifier should be modeled as Map<String, String>
  @FieldModifier(type = FieldModifier.Type.PROVIDED, valuesProvider = TypesProvider.class)
  @ConfigDef(
    name = "streetAddress",
    defaultValue = "180 Sansome",
    label = "street_address",
    required = true,
    description = "The domain of the twitter user",
    type = ConfigDef.Type.MODEL)
  public String streetAddress;

  //10. Both FieldSelector and FieldModifier present
  @FieldSelector
  @FieldModifier(type = FieldModifier.Type.PROVIDED, valuesProvider = TypesProvider.class)
  @ConfigDef(
    name = "ste",
    defaultValue = "400",
    label = "ste",
    required = true,
    description = "The domain of the twitter user",
    type = ConfigDef.Type.MODEL)
  public List<String> ste;

  //11. The ValuesProvider implementation used here does not implement the interface
  @FieldModifier(type = FieldModifier.Type.PROVIDED, valuesProvider = DatatypeProvider.class)
  @ConfigDef(
    name = "areaCode",
    defaultValue = "650",
    label = "area_code",
    required = true,
    description = "The area code",
    type = ConfigDef.Type.MODEL)
  public Map<String, String> areaCode;

}
