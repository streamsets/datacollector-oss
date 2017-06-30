/**
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
package com.streamsets.pipeline.stage.origin.opcua;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfig;
import org.eclipse.milo.opcua.sdk.client.api.identity.AnonymousProvider;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.stack.client.UaTcpStackClient;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UByte;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.ULong;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UShort;
import org.eclipse.milo.opcua.stack.core.types.enumerated.BrowseDirection;
import org.eclipse.milo.opcua.stack.core.types.enumerated.BrowseResultMask;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowseDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowseResult;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.eclipse.milo.opcua.stack.core.types.structured.ReferenceDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;
import static org.eclipse.milo.opcua.stack.core.util.ConversionUtil.toList;

public class OpcUaClientSource implements PushSource {

  private static final Logger LOG = LoggerFactory.getLogger(OpcUaClientSource.class);
  private final OpcUaClientSourceConfigBean conf;
  private Context context;
  private AtomicLong counter = new AtomicLong();
  private BlockingQueue<Exception> errorQueue;
  private List<Exception> errorList;
  private final AtomicLong clientHandles = new AtomicLong(1L);
  private OpcUaClient opcUaClient;
  private List<NodeId> nodeIds;

  OpcUaClientSource(OpcUaClientSourceConfigBean conf) {
    this.conf = conf;
  }

  @Override
  public int getNumberOfThreads() {
    return 1;
  }

  @Override
  public List<ConfigIssue> init(Info info, Context context) {
    List<ConfigIssue> issues = new ArrayList<>();
    this.context = context;
    errorQueue = new ArrayBlockingQueue<>(100);
    errorList = new ArrayList<>(100);

    try {
      opcUaClient = createClient();
      opcUaClient.connect().get();
    } catch (Exception ex) {
      issues.add(
          context.createConfigIssue(
              Groups.OPC_UA.name(),
              "conf.resourceUrl",
              Errors.OPC_UA_02,
              ex.getLocalizedMessage()
          )
      );
    }

    try {
      nodeIds = new ArrayList<>();
      conf.nodeIdConfigs.forEach(nodeIdConfig -> {
        switch (nodeIdConfig.identifierType) {
          case NUMERIC:
            nodeIds.add(new NodeId(nodeIdConfig.namespaceIndex, Integer.parseInt(nodeIdConfig.identifier)));
            break;
          case STRING:
            nodeIds.add(new NodeId(nodeIdConfig.namespaceIndex, nodeIdConfig.identifier));
            break;
          case UUID:
            nodeIds.add(new NodeId(nodeIdConfig.namespaceIndex, UUID.fromString(nodeIdConfig.identifier)));
            break;
          case OPAQUE:
            nodeIds.add(new NodeId(nodeIdConfig.namespaceIndex, new ByteString(nodeIdConfig.identifier.getBytes())));
            break;
        }
      });
    } catch (Exception ex) {
      issues.add(
          context.createConfigIssue(
              Groups.NODE_IDS.name(),
              "conf.nodeIdConfigs",
              Errors.OPC_UA_04,
              ex.getLocalizedMessage()
          )
      );
    }

    if (conf.nodeIdConfigs.isEmpty() && conf.readMode != OpcUaReadMode.BROWSE_NODES) {
      issues.add(
          context.createConfigIssue(
              Groups.NODE_IDS.name(),
              "conf.nodeIdConfigs",
              Errors.OPC_UA_03
          )
      );
    }

    return issues;
  }

  private OpcUaClient createClient() throws Exception {
    SecurityPolicy securityPolicy = SecurityPolicy.None;

    EndpointDescription[] endpoints = UaTcpStackClient.getEndpoints(conf.resourceUrl).get();

    EndpointDescription endpoint = Arrays.stream(endpoints)
        .filter(e -> e.getSecurityPolicyUri().equals(securityPolicy.getSecurityPolicyUri()))
        .findFirst().orElseThrow(() -> new StageException(Errors.OPC_UA_01));

    OpcUaClientConfig config = OpcUaClientConfig.builder()
        .setApplicationName(LocalizedText.english(conf.applicationName))
        .setApplicationUri(conf.applicationUri)
        .setEndpoint(endpoint)
        .setIdentityProvider(new AnonymousProvider())
        .setRequestTimeout(uint(conf.requestTimeoutMillis))
        .build();

    return new OpcUaClient(config);
  }

  @Override
  public void produce(Map<String, String> map, int i) throws StageException {
    try {
      switch (conf.readMode) {
        case POLLING:
          pollForData();
          break;
        case SUBSCRIBE:
          subscribeForData();
          break;
        case BROWSE_NODES:
          browseNodes();
          return;
      }
      while (!context.isStopped()) {
        dispatchReceiverErrors(100);
      }

    } catch(Exception me) {
      throw new StageException(Errors.OPC_UA_02, me, me);
    }
  }

  private void pollForData() {
    opcUaClient.readValues(0.0, TimestampsToReturn.Both, nodeIds)
        .thenAccept(values -> {
          try {
            process(values, System.currentTimeMillis() + "." + counter.getAndIncrement());
          } catch (Exception ex) {
            errorQueue.offer(ex);
          }
          if (ThreadUtil.sleep(conf.pollingInterval)) {
            pollForData();
          }
        });
  }

  private void subscribeForData() {
    try {
      UaSubscription subscription = opcUaClient.getSubscriptionManager()
          .createSubscription(1000.0).get();

      List<MonitoredItemCreateRequest> monitoredItemCreateRequests = new ArrayList<>();
      nodeIds.forEach(nodeId -> {
        ReadValueId readValueId = new ReadValueId(
            nodeId,
            AttributeId.Value.uid(),
            null,
            QualifiedName.NULL_VALUE
        );

        UInteger clientHandle = uint(clientHandles.getAndIncrement());

        MonitoringParameters parameters = new MonitoringParameters(
            clientHandle,
            1000.0,     // sampling interval
            null,       // filter, null means use default
            uint(10),   // queue size
            true        // discard oldest
        );

        MonitoredItemCreateRequest request = new MonitoredItemCreateRequest(
            readValueId, MonitoringMode.Reporting, parameters);

        monitoredItemCreateRequests.add(request);
      });

      BiConsumer<UaMonitoredItem, Integer> onItemCreated =
          (item, id) -> item.setValueConsumer(this::onSubscriptionValue);

      List<UaMonitoredItem> items = subscription.createMonitoredItems(
          TimestampsToReturn.Both,
          monitoredItemCreateRequests,
          onItemCreated
      ).get();

      for (UaMonitoredItem item : items) {
        if (item.getStatusCode().isGood()) {
          LOG.debug("item created for nodeId={}", item.getReadValueId().getNodeId());
        } else {
          LOG.error(Errors.OPC_UA_05.getMessage(), item.getReadValueId().getNodeId(), item.getStatusCode());
          errorQueue.offer(new StageException(Errors.OPC_UA_05, item.getReadValueId().getNodeId(), item.getStatusCode()));
        }
      }

    } catch (InterruptedException | ExecutionException ex) {
      errorQueue.offer(ex);
    }
  }

  private void onSubscriptionValue(UaMonitoredItem item, DataValue value) {
    LOG.debug(
        "subscription value received: item={}, value={}",
        item.getReadValueId().getNodeId(), value.getValue());
    process(ImmutableList.of(value), item.getReadValueId().getNodeId() + "." + counter.getAndIncrement());
  }

  private void process(List<DataValue> dataValues, String recordSourceId) {
    BatchContext batchContext = context.startBatch();
    Record record = context.createRecord(recordSourceId);
    record.set("/", Field.create(new LinkedHashMap<>()));

    int[] idx = { 0 };
    dataValues.forEach((dataValue) -> {
      NodeIdConfig nodeIdConfig = conf.nodeIdConfigs.get(idx[0]++);
      String fieldName = nodeIdConfig.field;
      if (!fieldName.startsWith("/")) {
        fieldName = "/" + fieldName;
      }

      Field.Type fieldType = Field.Type.STRING;
      Object value = dataValue.getValue().getValue();
      Optional<NodeId> nodeIdOptional =  dataValue.getValue().getDataType();
      int typeId = 12; // default to string type
      if (nodeIdOptional.isPresent()) {
        UInteger identifier = (UInteger)nodeIdOptional.get().getIdentifier();
        typeId = identifier.intValue();
      }

      boolean isArray = false;
      Object [] arrValue = null;
      if (value != null && value.getClass().isArray()) {
        isArray = true;
        arrValue = (Object []) value;
        fieldType = Field.Type.LIST;
      }

      switch (typeId) {
        case 1: // Boolean
          if (isArray) {
            List<Field> boolFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              boolFieldList.add(Field.create(Field.Type.BOOLEAN, anArrValue));
            }
            value = boolFieldList;
          } else {
            fieldType = Field.Type.BOOLEAN;
          }
          break;
        case 2: // SByte
          if (isArray) {
            List<Field> sByteFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              sByteFieldList.add(Field.create(Field.Type.BYTE, anArrValue));
            }
            value = sByteFieldList;
          } else {
            fieldType = Field.Type.BYTE;
          }
          break;
        case 3: // Byte
          if (isArray) {
            List<Field> byteFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              if (anArrValue != null) {
                UByte uByte = (UByte) anArrValue;
                anArrValue = uByte.byteValue();
              }
              byteFieldList.add(Field.create(Field.Type.BYTE, anArrValue));
            }
            value = byteFieldList;
          } else {
            fieldType = Field.Type.BYTE;
            if (value != null) {
              UByte uByte = (UByte) value;
              value = uByte.byteValue();
            }
          }
          break;
        case 4: // Int16
          if (isArray) {
            List<Field> integerFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              integerFieldList.add(Field.create(Field.Type.INTEGER, anArrValue));
            }
            value = integerFieldList;
          } else {
            fieldType = Field.Type.INTEGER;
          }
          break;
        case 5: // UInt16
          if (isArray) {
            List<Field> integerFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              if (anArrValue != null) {
                UShort uShort = (UShort) anArrValue;
                anArrValue = uShort.intValue();
              }
              integerFieldList.add(Field.create(Field.Type.INTEGER, anArrValue));
            }
            value = integerFieldList;
          } else {
            fieldType = Field.Type.INTEGER;
            if (value != null) {
              UShort uShort = (UShort) value;
              value = uShort.intValue();
            }
          }
          break;
        case 6: // Int32
          if (isArray) {
            List<Field> integerFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              integerFieldList.add(Field.create(Field.Type.INTEGER, anArrValue));
            }
            value = integerFieldList;
          } else {
            fieldType = Field.Type.INTEGER;
          }

          break;
        case 7: // UInt32
          if (isArray) {
            List<Field> integerFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              if (anArrValue != null) {
                UInteger uInteger = (UInteger) anArrValue;
                anArrValue = uInteger.intValue();
              }
              integerFieldList.add(Field.create(Field.Type.INTEGER, anArrValue));
            }
            value = integerFieldList;
          } else {
            fieldType = Field.Type.INTEGER;
            if (value != null) {
              UInteger uInteger = (UInteger) value;
              value = uInteger.intValue();
            }
          }
          break;
        case 8: // Int64
          if (isArray) {
            List<Field> longFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              longFieldList.add(Field.create(Field.Type.LONG, anArrValue));
            }
            value = longFieldList;
          } else {
            fieldType = Field.Type.LONG;
          }
          break;
        case 9: // UInt64
          if (isArray) {
            List<Field> longFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              if (anArrValue != null) {
                ULong uLong = (ULong) anArrValue;
                anArrValue = uLong.longValue();
              }
              longFieldList.add(Field.create(Field.Type.LONG, anArrValue));
            }
            value = longFieldList;
          } else {
            fieldType = Field.Type.LONG;
            if (value != null) {
              ULong uLong = (ULong) value;
              value = uLong.longValue();
            }
          }
          break;
        case 10: // Float
          if (isArray) {
            List<Field> floatFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              floatFieldList.add(Field.create(Field.Type.FLOAT, anArrValue));
            }
            value = floatFieldList;
          } else {
            fieldType = Field.Type.FLOAT;
          }
          break;
        case 11: // Double
          if (isArray) {
            List<Field> doubleFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              doubleFieldList.add(Field.create(Field.Type.DOUBLE, anArrValue));
            }
            value = doubleFieldList;
          } else {
            fieldType = Field.Type.DOUBLE;
          }
          break;
        case 12: // String
          if (isArray) {
            List<Field> stringFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              stringFieldList.add(Field.create(Field.Type.STRING, anArrValue));
            }
            value = stringFieldList;
          } else {
            fieldType = Field.Type.STRING;
          }
          break;
        case 13: // DateTime
          if (isArray) {
            List<Field> dateTimeFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              if (anArrValue != null) {
                DateTime dateTime = (DateTime) anArrValue;
                anArrValue = dateTime.getJavaDate();
              }
              dateTimeFieldList.add(Field.create(Field.Type.DATETIME, anArrValue));
            }
            value = dateTimeFieldList;
          } else {
            fieldType = Field.Type.DATETIME;
            if (value != null) {
              DateTime dateTime = (DateTime) value;
              value = dateTime.getJavaDate();
            }
          }
          break;
        case 14: // Guid
          if (isArray) {
            List<Field> guidFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              if (anArrValue != null) {
                anArrValue = anArrValue.toString();
              }
              guidFieldList.add(Field.create(Field.Type.STRING, anArrValue));
            }
            value = guidFieldList;
          } else {
            fieldType = Field.Type.STRING;
            if (value != null) {
              value = value.toString();
            }
          }
          break;
        case 15: // ByteString
          if (isArray) {
            List<Field> byteStringFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              if (anArrValue != null) {
                ByteString byteString = (ByteString)anArrValue;
                anArrValue = byteString.bytes();
              }
              byteStringFieldList.add(Field.create(Field.Type.BYTE_ARRAY, anArrValue));
            }
            value = byteStringFieldList;
          } else {
            fieldType = Field.Type.BYTE_ARRAY;
            if (value != null) {
              ByteString byteString = (ByteString)value;
              value = byteString.bytes();
            }
          }
          break;
        default:
          if (isArray) {
            List<Field> stringFieldList = new ArrayList<>();
            for (Object anArrValue : arrValue) {
              stringFieldList.add(Field.create(Field.Type.STRING, anArrValue));
            }
            value = stringFieldList;
          } else {
            fieldType = Field.Type.STRING;
          }
      }

      record.set(fieldName, Field.create(fieldType, value));
    });

    batchContext.getBatchMaker().addRecord(record);
    context.processBatch(batchContext);
  }

  private void dispatchReceiverErrors(long intervalMillis) {
    if (intervalMillis > 0) {
      try {
        Thread.sleep(intervalMillis);
      } catch (InterruptedException ignored) {
      }
    }
    errorList.clear();
    errorQueue.drainTo(errorList);
    for (Exception exception : errorList) {
      context.reportError(exception);
    }
  }

  private void browseNodes() {
    LinkedHashMap<String, Field> rootFieldMap = new LinkedHashMap<>();
    browseNode(Identifiers.RootFolder, rootFieldMap);
    BatchContext batchContext = context.startBatch();
    String requestId = System.currentTimeMillis() + "." + counter.getAndIncrement();
    Record record = context.createRecord(requestId);
    record.set("/", Field.create(rootFieldMap));
    batchContext.getBatchMaker().addRecord(record);
    context.processBatch(batchContext);
  }

  private void browseNode(NodeId browseRoot, LinkedHashMap<String, Field> rootFieldMap) {
    BrowseDescription browse = new BrowseDescription(
        browseRoot,
        BrowseDirection.Forward,
        Identifiers.References,
        true,
        uint(NodeClass.Object.getValue() | NodeClass.Variable.getValue()),
        uint(BrowseResultMask.All.getValue())
    );

    try {
      BrowseResult browseResult = opcUaClient.browse(browse).get();
      List<ReferenceDescription> references = toList(browseResult.getReferences());

      for (ReferenceDescription rd : references) {
        LinkedHashMap<String, Field> fieldMap = new LinkedHashMap<>();
        fieldMap.put("name", Field.create(rd.getBrowseName().getName()));
        fieldMap.put("nameSpaceIndex", Field.create(rd.getNodeId().getNamespaceIndex().intValue()));
        fieldMap.put("identifierType", Field.create(rd.getNodeId().getType().name()));
        fieldMap.put("identifier", Field.create(rd.getNodeId().getIdentifier().toString()));

        // recursively browse to children
        rd.getNodeId().local().ifPresent(nodeId -> {
          LinkedHashMap<String, Field> childrenMap = new LinkedHashMap<>();
          browseNode(nodeId, childrenMap);
          fieldMap.put("children",  Field.create(childrenMap));
        });

        rootFieldMap.put(rd.getBrowseName().getName(), Field.create(fieldMap));
      }
    } catch (InterruptedException | ExecutionException ex) {
      LOG.error("Browsing nodeId={} failed: {}", browseRoot, ex.getMessage(), ex);
      errorQueue.offer(ex);
    }
  }

  @Override
  public void destroy() {
    if (opcUaClient != null) {
      try {
        opcUaClient.disconnect().get();
      } catch (InterruptedException | ExecutionException ex) {
        LOG.error("Failed during OPC UA Client disconnect call: {}", ex.getMessage());
      }
    }
  }

}
