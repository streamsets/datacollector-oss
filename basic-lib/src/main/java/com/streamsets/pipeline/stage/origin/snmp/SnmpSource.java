/*
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.snmp;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.snmp.SnmpHostConfig;
import com.streamsets.pipeline.lib.snmp.TransportType;
import org.snmp4j.CommunityTarget;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.TransportMapping;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.smi.Address;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.Variable;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultTcpTransportMapping;
import org.snmp4j.transport.DefaultUdpTransportMapping;

import java.io.IOException;
import java.util.List;

public class SnmpSource extends BaseSource implements OffsetCommitter {
  private final SnmpConfig conf;

  private Snmp snmp;

  public SnmpSource(SnmpConfig conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    for (SnmpHostConfig host : conf.hosts) {
      try {
        snmp = new Snmp(getTransport(host.transportType));
        Address address = new UdpAddress(host + "/161"); // SNMP UDP Port
        CommunityTarget target = new CommunityTarget();
        target.setAddress(address);
        target.setTimeout(500);
        target.setRetries(3);
        target.setCommunity(new OctetString("public"));
        target.setVersion(host.version.getValue());

        PDU request = new PDU();
        request.setType(PDU.GET);
        OID oid = new OID(".1.3.6.1.2.1.1.1.0");
        request.add(new VariableBinding(oid));

        PDU responsePDU;
        ResponseEvent responseEvent;
        responseEvent = snmp.send(request, target);

        if (responseEvent != null) {
          responsePDU = responseEvent.getResponse();
          if (responsePDU != null) {
            List<? extends VariableBinding> tmpv = responsePDU.getVariableBindings();
            if (tmpv != null) {
              for (VariableBinding vb : tmpv) {
                if (vb.isException()) {
                  // handle error
                } else {
                  String sOid = vb.getOid().toString();
                  Variable var = vb.getVariable();
                  OctetString oct = new OctetString((OctetString) var);
                  String sVar = oct.toString();
                }
              }
            }
          }
        }
      } catch (IOException e) {
        // TODO
      }
    }
    return issues;
  }

  private TransportMapping<? extends Address> getTransport(TransportType transportType) throws IOException {
    switch (transportType) {
      case UDP:
        return new DefaultUdpTransportMapping();
      case TCP:
        return new DefaultTcpTransportMapping();
      case SSH:
        //        return new DefaultSshTransportMapping();
      default:
        throw new IllegalArgumentException("Unsupported transport: " + transportType);
    }
  }

  @Override
  public void destroy() {
    super.destroy();
  }

  @Override
  public void commit(String offset) throws StageException {
    // no-op
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    String nextSourceOffset = lastSourceOffset;



    return nextSourceOffset;
  }
}
