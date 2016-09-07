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

import com.google.api.client.util.Sets;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.AbstractResponse;
import com.maxmind.geoip2.model.AnonymousIpResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.ConnectionTypeResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.model.DomainResponse;
import com.maxmind.geoip2.model.IspResponse;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.impl.Utils;

import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class  GeolocationProcessor extends SingleLaneRecordProcessor {
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

  private final List<GeolocationDatabaseConfig> dbConfigs;
  private final List<GeolocationFieldConfig> configs;
  private final GeolocationMissingAddressAction missingAddressAction;
  private Map<GeolocationDBType, DatabaseReader> readers = Maps.newHashMap();
  LoadingCache<Field, Map<GeolocationDBType, AbstractResponse>> responseCache;
  private DefaultErrorRecordHandler errorRecordHandler;

  public GeolocationProcessor(
      List<GeolocationDatabaseConfig> dbConfigs,
      GeolocationMissingAddressAction missingAddressAction,
      List<GeolocationFieldConfig> configs
  ) {
    this.dbConfigs = dbConfigs;
    this.missingAddressAction = missingAddressAction;
    this.configs = configs;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> result = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    // Will be used to determine the full set of supported fields for the provided DBs
    Set<GeolocationField> supportedFields = Sets.newHashSet();

    // Validate each database file
    for (GeolocationDatabaseConfig dbConfig : dbConfigs) {
      File database = new File(dbConfig.geoIP2DBFile);
      if ((getContext().getExecutionMode() == ExecutionMode.CLUSTER_BATCH
          || getContext().getExecutionMode() == ExecutionMode.CLUSTER_YARN_STREAMING
          || getContext().getExecutionMode() == ExecutionMode.CLUSTER_MESOS_STREAMING)
          && database.isAbsolute()) {
        //Do not allow absolute geoIP2DBFile in cluster mode
        result.add(getContext().createConfigIssue("GEOLOCATION", "dbConfigs", Errors.GEOIP_10, dbConfig.geoIP2DBFile));
      } else {
        if (!database.isAbsolute()) {
          database = new File(getContext().getResourcesDirectory(), dbConfig.geoIP2DBFile).getAbsoluteFile();
        }
        if (database.isFile()) {
          try {
            // The MaxMind APIs require making specific calls to get results for different databases. This unfortunately
            // prevents much in the way of generalizing field retrieval, and forces us to have switch statements all over
            // the place.
            DatabaseReader reader = new DatabaseReader.Builder(database).build();
            supportedFields.addAll(dbConfig.geoIP2DBType.supportedFields);
            switch (dbConfig.geoIP2DBType) {
              case COUNTRY:
                reader.country(KNOWN_GOOD_ADDRESS);
                break;
              case CITY:
                reader.city(KNOWN_GOOD_ADDRESS);
                break;
              case DOMAIN:
                reader.domain(KNOWN_GOOD_ADDRESS);
                break;
              case ANONYMOUS_IP:
                reader.anonymousIp(KNOWN_GOOD_ADDRESS);
                break;
              case ISP:
                reader.isp(KNOWN_GOOD_ADDRESS);
                break;
              case CONNECTION_TYPE:
                reader.connectionType(KNOWN_GOOD_ADDRESS);
                break;
              default:
                throw new IllegalStateException(Utils.format("Unknown configuration value: ", dbConfig.geoIP2DBType));
            }

            // Store the reader for later -- currently only supports one database per type, but it's unclear if there's
            // any reason to have more than one per type.
            readers.put(dbConfig.geoIP2DBType, reader);
          } catch (IOException ex) {
            result.add(getContext().createConfigIssue("GEOLOCATION", "dbConfigs", Errors.GEOIP_01, database.getPath(),
                ex));
            LOG.info(Utils.format(Errors.GEOIP_01.getMessage(), ex), ex);
          } catch (UnsupportedOperationException ex) {
            result.add(getContext().createConfigIssue("GEOLOCATION", "dbConfigs", Errors.GEOIP_05,
                dbConfig.geoIP2DBFile, dbConfig.geoIP2DBType));
            LOG.info(Utils.format(Errors.GEOIP_05.getMessage(), dbConfig.geoIP2DBFile, dbConfig.geoIP2DBType), ex);
          } catch (GeoIp2Exception ex) {
            result.add(getContext().createConfigIssue("GEOLOCATION", "dbConfigs", Errors.GEOIP_07,
                ex));
            LOG.error(Utils.format(Errors.GEOIP_07.getMessage(), ex), ex);
          }
        } else {
          result.add(getContext().createConfigIssue("GEOLOCATION", "dbConfigs", Errors.GEOIP_00, dbConfig.geoIP2DBFile));
        }
      }
    }

    for (GeolocationFieldConfig config : this.configs) {
      if (!supportedFields.contains(config.targetType)) {
        result.add(getContext().createConfigIssue("GEOLOCATION", "fieldTypeConverterConfigs", Errors.GEOIP_12,
            config.targetType, config.targetType.supportedDbTypes));
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

    createResponseCache();

    return result;
  }

  private void createResponseCache() {
    responseCache = CacheBuilder.newBuilder().maximumSize(1000).build(
        new CacheLoader<Field, Map<GeolocationDBType, AbstractResponse>>() {
          @Override
          public Map<GeolocationDBType, AbstractResponse> load(Field field) throws Exception {
            Map<GeolocationDBType, AbstractResponse> responses = Maps.newHashMap();
            // Each time we load an entry, we'll opportunistically just load that entry for all
            // available databases.
            for (Map.Entry<GeolocationDBType, DatabaseReader> entry : readers.entrySet()) {
              DatabaseReader reader = Utils.checkNotNull(entry.getValue(), "DatabaseReader");
              AbstractResponse resp = null;
              switch (entry.getKey()) {
                case COUNTRY:
                  resp = reader.country(toAddress(field));
                  break;
                case CITY:
                  resp = reader.city(toAddress(field));
                  break;
                case ANONYMOUS_IP:
                  resp = reader.anonymousIp(toAddress(field));
                  break;
                case DOMAIN:
                  resp = reader.domain(toAddress(field));
                  break;
                case ISP:
                  resp = reader.isp(toAddress(field));
                  break;
                case CONNECTION_TYPE:
                  resp =  reader.connectionType(toAddress(field));
                  break;
              }
              responses.put(entry.getKey(), resp);
            }

            return responses;
          }
        });
  }

  @Override
  public void destroy() {
    for (DatabaseReader reader : readers.values()) {
      IOUtils.closeQuietly(reader);
    }
    super.destroy();
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    try {
      for (GeolocationFieldConfig config : configs) {
        Field field = record.get(config.inputFieldName);

        if(field == null) {
          errorRecordHandler.onError(new OnRecordErrorException(record, Errors.GEOIP_11, record.getHeader().getSourceId(), config.inputFieldName));
          return;
        }

        try {
          Map<GeolocationDBType, AbstractResponse> responses = responseCache.get(field);
          switch (config.targetType) {
            // Multiple databases support country name and ISO code, so we need to figure out which ones are available
            case COUNTRY_NAME:
              String name = null;
              if (responses.containsKey(GeolocationDBType.COUNTRY)) {
                CountryResponse countryResp = (CountryResponse) responses.get(GeolocationDBType.COUNTRY);
                name = countryResp.getCountry().getName();
              } else {
                CityResponse cityResp = (CityResponse) responses.get(GeolocationDBType.CITY);
                name = cityResp.getCountry().getName();
              }
              record.set(config.outputFieldName, Field.create(name));
              break;
            case COUNTRY_ISO_CODE:
              String isoCode = null;
              if (responses.containsKey(GeolocationDBType.COUNTRY)) {
                CountryResponse countryResp = (CountryResponse) responses.get(GeolocationDBType.COUNTRY);
                isoCode = countryResp.getCountry().getIsoCode();
              } else {
                CityResponse cityResp = (CityResponse) responses.get(GeolocationDBType.CITY);
                isoCode = cityResp.getCountry().getIsoCode();
              }
              record.set(config.outputFieldName, Field.create(isoCode));
              break;
            case CITY_NAME:
              CityResponse cityResp = (CityResponse) responses.get(GeolocationDBType.CITY);
              record.set(config.outputFieldName, Field.create(cityResp.getCity().getName()));
              break;
            case LATITUDE:
              cityResp = (CityResponse) responses.get(GeolocationDBType.CITY);
              record.set(config.outputFieldName, Field.create(cityResp.getLocation().getLatitude()));
              break;
            case LONGITUDE:
              cityResp = (CityResponse) responses.get(GeolocationDBType.CITY);
              record.set(config.outputFieldName, Field.create(cityResp.getLocation().getLongitude()));
              break;
            case IS_ANONYMOUS:
              AnonymousIpResponse anonResp = (AnonymousIpResponse) responses.get(GeolocationDBType.ANONYMOUS_IP);
              record.set(config.outputFieldName, Field.create(anonResp.isAnonymous()));
              break;
            case IS_ANONYMOUS_VPN:
              anonResp = (AnonymousIpResponse) responses.get(GeolocationDBType.ANONYMOUS_IP);
              record.set(config.outputFieldName, Field.create(anonResp.isAnonymousVpn()));
              break;
            case IS_HOSTING_PROVIDER:
              anonResp = (AnonymousIpResponse) responses.get(GeolocationDBType.ANONYMOUS_IP);
              record.set(config.outputFieldName, Field.create(anonResp.isHostingProvider()));
              break;
            case IS_PUBLIC_PROXY:
              anonResp = (AnonymousIpResponse) responses.get(GeolocationDBType.ANONYMOUS_IP);
              record.set(config.outputFieldName, Field.create(anonResp.isPublicProxy()));
              break;
            case IS_TOR_EXIT_NODE:
              anonResp = (AnonymousIpResponse) responses.get(GeolocationDBType.ANONYMOUS_IP);
              record.set(config.outputFieldName, Field.create(anonResp.isTorExitNode()));
              break;
            case DOMAIN:
              DomainResponse domainResp = (DomainResponse) responses.get(GeolocationDBType.DOMAIN);
              record.set(config.outputFieldName, Field.create(domainResp.getDomain()));
              break;
            case CONNECTION_TYPE:
              ConnectionTypeResponse connResp = (ConnectionTypeResponse) responses.get(GeolocationDBType.CONNECTION_TYPE);
              record.set(config.outputFieldName, Field.create(connResp.getConnectionType().toString()));
              break;
            case AUTONOMOUS_SYSTEM_NUMBER:
              IspResponse ispResp = (IspResponse) responses.get(GeolocationDBType.ISP);
              record.set(config.outputFieldName, Field.create(ispResp.getAutonomousSystemNumber()));
              break;
            case AUTONOMOUS_SYSTEM_ORG:
              ispResp = (IspResponse) responses.get(GeolocationDBType.ISP);
              record.set(config.outputFieldName, Field.create(ispResp.getAutonomousSystemOrganization()));
              break;
            case ISP:
              ispResp = (IspResponse) responses.get(GeolocationDBType.ISP);
              record.set(config.outputFieldName, Field.create(ispResp.getIsp()));
              break;
            case ORGANIZATION:
              ispResp = (IspResponse) responses.get(GeolocationDBType.ISP);
              record.set(config.outputFieldName, Field.create(ispResp.getOrganization()));
              break;

            case CITY_FULL_JSON:
              cityResp = (CityResponse) responses.get(GeolocationDBType.CITY);
              record.set(config.outputFieldName, Field.create(cityResp.toJson()));
              break;
            case COUNTRY_FULL_JSON:
              CountryResponse countryResp = (CountryResponse) responses.get(GeolocationDBType.COUNTRY);
              record.set(config.outputFieldName, Field.create(countryResp.toJson()));
              break;
            case ANONYMOUS_IP_FULL_JSON:
              anonResp = (AnonymousIpResponse) responses.get(GeolocationDBType.ANONYMOUS_IP);
              record.set(config.outputFieldName, Field.create(anonResp.toJson()));
              break;
            case DOMAIN_FULL_JSON:
              domainResp = (DomainResponse) responses.get(GeolocationDBType.DOMAIN);
              record.set(config.outputFieldName, Field.create(domainResp.toJson()));
              break;
            case CONNECTION_TYPE_FULL_JSON:
              connResp = (ConnectionTypeResponse) responses.get(GeolocationDBType.CONNECTION_TYPE);
              record.set(config.outputFieldName, Field.create(connResp.toJson()));
              break;
            case ISP_FULL_JSON:
              ispResp = (IspResponse) responses.get(GeolocationDBType.ISP);
              record.set(config.outputFieldName, Field.create(ispResp.toJson()));
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
            switch (missingAddressAction) {
              case TO_ERROR:
                errorRecordHandler.onError(new OnRecordErrorException(record, Errors.GEOIP_02, field.getValue(), cause.getMessage()));
                LOG.debug(Utils.format(Errors.GEOIP_02.getMessage(), field.getValue(), cause.getMessage()), cause);
                continue;
              case REPLACE_WITH_NULLS:
                record.set(config.outputFieldName, Field.create(config.targetType.fieldType, null));
              case IGNORE:
                continue;
              default:
                throw new IllegalStateException(Utils.format("Unknown configuration value: ", missingAddressAction));
            }
          }
          Throwables.propagateIfInstanceOf(cause, GeoIp2Exception.class);
          Throwables.propagateIfInstanceOf(cause, IOException.class);
          Throwables.propagate(cause);
        }
      }
    } catch (GeoIp2Exception ex) {
      throw new StageException(Errors.GEOIP_03, ex);
    } catch (IOException ex) {
      throw new StageException(Errors.GEOIP_01, ex);
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
