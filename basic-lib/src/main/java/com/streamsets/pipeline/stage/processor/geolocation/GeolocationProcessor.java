/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.processor.geolocation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.impl.Utils;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class GeolocationProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(GeolocationProcessor.class);
  private static final InetAddress KNOWN_GOOD_ADDRESS;
  static {
    try {
      KNOWN_GOOD_ADDRESS = InetAddress.getByAddress(new byte[]{(byte)8, (byte)8, (byte)8, (byte)8});
    } catch (UnknownHostException e) {
      // this cannot happen
      throw new IllegalStateException("Unexpected exception: " + e, e);
    }
  }

  private final String geoIP2DBFile;
  private final List<GeolocationFieldConfig> configs;
  private LoadingCache<Field, CountryResponse> countries;
  private LoadingCache<Field, CityResponse> cities;
  private DatabaseReader reader;

  public GeolocationProcessor(String geoIP2DBFile, List<GeolocationFieldConfig> configs) {
    this.geoIP2DBFile = geoIP2DBFile;
    this.configs = configs;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> result = super.init();
    File database = new File(geoIP2DBFile);
    if ((getContext().getExecutionMode() == ExecutionMode.CLUSTER_BATCH
        || getContext().getExecutionMode() == ExecutionMode.CLUSTER_YARN_STREAMING
        || getContext().getExecutionMode() == ExecutionMode.CLUSTER_MESOS_STREAMING)
      && database.isAbsolute()) {
    //Do not allow absolute geoIP2DBFile in cluster mode
      result.add(getContext().createConfigIssue("GEOLOCATION", "geoIP2DBFile", Errors.GEOIP_10, geoIP2DBFile));
    } else {
      if (!database.isAbsolute()) {
        database = new File(getContext().getResourcesDirectory(), geoIP2DBFile).getAbsoluteFile();
      }
      if (database.isFile()) {
        try {
          reader = new DatabaseReader.Builder(database).build();
          for (GeolocationFieldConfig config : configs) {
            try {
              switch (config.targetType) {
                case COUNTRY_NAME:
                case COUNTRY_ISO_CODE:
                  reader.country(KNOWN_GOOD_ADDRESS);
                  break;
                case CITY_NAME:
                case LATITUDE:
                case LONGITUDE:
                  reader.city(KNOWN_GOOD_ADDRESS);
                  break;
                default:
                  throw new IllegalStateException(Utils.format("Unknown configuration value: ", config.targetType));
              }
            } catch (UnsupportedOperationException ex) {
              result.add(getContext().createConfigIssue("GEOLOCATION", "geoIP2DBFile", Errors.GEOIP_05,
                config.targetType.name()));
              LOG.info(Utils.format(Errors.GEOIP_05.getMessage(), config.targetType.name()), ex);
            } catch (GeoIp2Exception ex) {
              result.add(getContext().createConfigIssue("GEOLOCATION", "geoIP2DBFile", Errors.GEOIP_07,
                ex));
              LOG.error(Utils.format(Errors.GEOIP_07.getMessage(), ex), ex);
            }
          }
        } catch (IOException ex) {
          result.add(getContext().createConfigIssue("GEOLOCATION", "geoIP2DBFile", Errors.GEOIP_01, database.getPath(),
            ex));
          LOG.info(Utils.format(Errors.GEOIP_01.getMessage(), ex), ex);
        }
      } else {
        result.add(getContext().createConfigIssue("GEOLOCATION", "geoIP2DBFile", Errors.GEOIP_00, geoIP2DBFile));
      }
    }
    if (configs.isEmpty()) {
      result.add(getContext().createConfigIssue("GEOLOCATION", "fieldTypeConverterConfigs", Errors.GEOIP_04));
    }
    for (GeolocationFieldConfig config : configs) {
      if (config.inputFieldName == null || config.inputFieldName.isEmpty()) {
        result.add(getContext().createConfigIssue("GEOLOCATION", "fieldTypeConverterConfigs", Errors.GEOIP_08));
      } else if (config.outputFieldName == null || config.outputFieldName.isEmpty()) {
        result.add(getContext().createConfigIssue("GEOLOCATION", "fieldTypeConverterConfigs", Errors.GEOIP_09));
      }
    }
    countries = CacheBuilder.newBuilder().maximumSize(1000).build(new CacheLoader<Field, CountryResponse>() {
      @Override
      public CountryResponse load(Field field) throws Exception {
        return Utils.checkNotNull(reader, "DatabaseReader").country(toAddress(field));
      }
    });
    cities = CacheBuilder.newBuilder().maximumSize(1000).build(new CacheLoader<Field, CityResponse>() {
      @Override
      public CityResponse load(Field field) throws Exception {
        return Utils.checkNotNull(reader, "DatabaseReader").city(toAddress(field));
      }
    });
    return result;
  }

  @Override
  public void destroy() {
    IOUtils.closeQuietly(reader);
    super.destroy();
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    try {
      for (GeolocationFieldConfig config : configs) {
        Field field = record.get(config.inputFieldName);

        if(field == null) {
          throw new OnRecordErrorException(Errors.GEOIP_11, record.getHeader().getSourceId(), config.inputFieldName);
        }

        try {
          switch (config.targetType) {
            case COUNTRY_NAME:
              CountryResponse countryName = countries.get(field);
              record.set(config.outputFieldName, Field.create(countryName.getCountry().getName()));
              break;
            case COUNTRY_ISO_CODE:
              CountryResponse countryIso = countries.get(field);
              record.set(config.outputFieldName, Field.create(countryIso.getCountry().getIsoCode()));
              break;
            case CITY_NAME:
              CityResponse cityName = cities.get(field);
              record.set(config.outputFieldName, Field.create(cityName.getCity().getName()));
              break;
            case LATITUDE:
              CityResponse cityLat = cities.get(field);
              if (cityLat.getLocation() != null && cityLat.getLocation().getLatitude() != null) {
                record.set(config.outputFieldName, Field.create(cityLat.getLocation().getLatitude()));
              }
              break;
            case LONGITUDE:
              CityResponse cityLong = cities.get(field);
              if (cityLong.getLocation() != null && cityLong.getLocation().getLatitude() != null) {
                record.set(config.outputFieldName, Field.create(cityLong.getLocation().getLongitude()));
              }
              break;
            default:
              throw new IllegalStateException(Utils.format("Unknown configuration value: ", config.targetType));
          }
        } catch (ExecutionException ex) {
          Throwable cause = ex.getCause();
          if (cause == null) {
            cause = ex;
          }
          if (cause instanceof UnknownHostException || cause instanceof AddressNotFoundException) {
            throw new OnRecordErrorException(Errors.GEOIP_02, field.getValue(), cause);
          }
          Throwables.propagateIfInstanceOf(cause, OnRecordErrorException.class);
          Throwables.propagateIfInstanceOf(cause, GeoIp2Exception.class);
          Throwables.propagateIfInstanceOf(cause, IOException.class);
          Throwables.propagate(cause);
        }
      }
    } catch (GeoIp2Exception ex) {
      throw new StageException(Errors.GEOIP_03, ex);
    } catch (IOException ex) {
      throw new StageException(Errors.GEOIP_01, geoIP2DBFile, ex);
    }
    batchMaker.addRecord(record);
  }

  @VisibleForTesting
  InetAddress toAddress(Field field) throws UnknownHostException, OnRecordErrorException {
    switch (field.getType()) {
      case LONG:
      case INTEGER:
        return InetAddress.getByAddress(ipAsIntToBytes(field.getValueAsInteger()));
      case STRING:
        String ip = field.getValueAsString();
        if(ip != null) {
          ip = ip.trim();
          if (ip.contains(".")) {
            return InetAddress.getByAddress(ipAsStringToBytes(ip));
          } else {
            try {
              return InetAddress.getByAddress(ipAsIntToBytes(Integer.parseInt(ip)));
            } catch (NumberFormatException nfe) {
              throw new OnRecordErrorException(Errors.GEOIP_06, ip, nfe);
            }
          }
        } else {
          throw new OnRecordErrorException(Errors.GEOIP_06, ip);
        }
      default:
        throw new IllegalStateException(Utils.format("Unknown field type: ", field.getType()));
    }
  }

  @VisibleForTesting
  static byte[] ipAsIntToBytes(int ip) {
    return new byte[] {
      (byte)(ip >> 24),
      (byte)(ip >> 16),
      (byte)(ip >> 8),
      (byte)(ip & 0xff)
    };
  }

  @VisibleForTesting
  static int ipAsBytesToInt(byte[] ip) {
    int result = 0;
    for (byte b: ip) {
      result = result << 8 | (b & 0xFF);
    }
    return result;
  }

  @VisibleForTesting
  static String ipAsIntToString(int ip) {
    return String.format("%d.%d.%d.%d",
      (ip >> 24 & 0xff),
      (ip >> 16 & 0xff),
      (ip >> 8 & 0xff),
      (ip & 0xff));
  }

  @VisibleForTesting
  static int ipAsStringToInt(String ip) throws OnRecordErrorException {
    try {
      int ipAsInt = 0;
      String[] parts = ip.trim().split("\\.");
      if (parts.length != 4) {
        throw new OnRecordErrorException(Errors.GEOIP_06, ip);
      }
      for (String byteString : parts) { // TODO validate 3 dots
        ipAsInt = (ipAsInt << 8) | Integer.parseInt(byteString);
      }
      return ipAsInt;
    } catch (NumberFormatException ex) {
      throw new OnRecordErrorException(Errors.GEOIP_06, ip);
    }
  }

  @VisibleForTesting
  static byte[] ipAsStringToBytes(String ip) throws OnRecordErrorException {
    return ipAsIntToBytes(ipAsStringToInt(ip));
  }
}
