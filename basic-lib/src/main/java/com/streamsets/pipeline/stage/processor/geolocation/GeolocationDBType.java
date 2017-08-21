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


import java.util.Arrays;
import java.util.List;

public enum GeolocationDBType {
  CITY(GeolocationField.CITY_NAME, GeolocationField.COUNTRY_ISO_CODE,
      GeolocationField.COUNTRY_NAME, GeolocationField.LATITUDE, GeolocationField.LONGITUDE,
      GeolocationField.CITY_FULL_JSON),
  COUNTRY(GeolocationField.COUNTRY_NAME, GeolocationField.COUNTRY_ISO_CODE, GeolocationField.COUNTRY_FULL_JSON),
  CONNECTION_TYPE(GeolocationField.CONNECTION_TYPE, GeolocationField.CONNECTION_TYPE_FULL_JSON),
  DOMAIN(GeolocationField.DOMAIN, GeolocationField.DOMAIN_FULL_JSON),
  ANONYMOUS_IP(GeolocationField.IS_ANONYMOUS, GeolocationField.IS_ANONYMOUS_VPN, GeolocationField.IS_HOSTING_PROVIDER,
      GeolocationField.IS_PUBLIC_PROXY, GeolocationField.IS_TOR_EXIT_NODE, GeolocationField.ANONYMOUS_IP_FULL_JSON),
  ISP(GeolocationField.ISP, GeolocationField.ISP_FULL_JSON);

  List<GeolocationField> supportedFields;

  GeolocationDBType(GeolocationField... fields) {
    this.supportedFields = Arrays.asList(fields);
    for (GeolocationField field : fields) {
      field.addSupportedDatabase(this);
    }
  }
}
