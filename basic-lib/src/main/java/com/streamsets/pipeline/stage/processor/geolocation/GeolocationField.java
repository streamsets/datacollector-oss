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
package com.streamsets.pipeline.stage.processor.geolocation;

import com.google.api.client.util.Lists;
import com.streamsets.pipeline.api.Field;

import java.util.List;

public enum GeolocationField {
  // City/Country Fields
  COUNTRY_NAME(Field.Type.STRING),
  COUNTRY_ISO_CODE(Field.Type.STRING),
  CITY_NAME(Field.Type.STRING),
  LONGITUDE(Field.Type.DOUBLE),
  LATITUDE(Field.Type.DOUBLE),
  COUNTRY_FULL_JSON(Field.Type.STRING),
  CITY_FULL_JSON(Field.Type.STRING),

  // Anonymous IP Fields
  IS_ANONYMOUS(Field.Type.BOOLEAN),
  IS_ANONYMOUS_VPN(Field.Type.BOOLEAN),
  IS_HOSTING_PROVIDER(Field.Type.BOOLEAN),
  IS_PUBLIC_PROXY(Field.Type.BOOLEAN),
  IS_TOR_EXIT_NODE(Field.Type.BOOLEAN),
  ANONYMOUS_IP_FULL_JSON(Field.Type.STRING),

  // Domain Fields
  DOMAIN(Field.Type.STRING),
  DOMAIN_FULL_JSON(Field.Type.STRING),

  // Connection Type Fields
  CONNECTION_TYPE(Field.Type.STRING),
  CONNECTION_TYPE_FULL_JSON(Field.Type.STRING),

  // ISP Fields
  AUTONOMOUS_SYSTEM_NUMBER(Field.Type.INTEGER),
  AUTONOMOUS_SYSTEM_ORG(Field.Type.STRING),
  ISP(Field.Type.STRING),
  ORGANIZATION(Field.Type.STRING),
  ISP_FULL_JSON(Field.Type.STRING),
  ;

  List<GeolocationDBType> supportedDbTypes = Lists.newArrayList();
  Field.Type fieldType;

  GeolocationField(Field.Type fieldType) {
    this.fieldType = fieldType;
  }

  void addSupportedDatabase(GeolocationDBType dbType) {
    this.supportedDbTypes.add(dbType);
  }
}
