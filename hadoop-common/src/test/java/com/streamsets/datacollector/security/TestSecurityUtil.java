/**
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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.security;

import java.io.FilePermission;
import java.security.AccessController;
import java.security.Permission;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Set;

import javax.security.auth.Subject;

import org.junit.After;
import org.junit.Test;

public class TestSecurityUtil {

  @After
  public void shutdown() {
    System.setSecurityManager(null);
  }

  // This test should not be flakey
  @Test(timeout=10000)
  public void testDeadLockJDKSecurityManager() {
    final Subject subject = new Subject();
    Subject.doAs(subject, new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        // set the security manager and override checkPermission as in java.lang.SecurityManager
        System.setSecurityManager(new SecurityManager() {
          @Override
          public void checkWrite(String fd) {
          }

          @Override
          public void checkPermission(Permission perm) {
            try {
              AccessController.checkPermission(perm);
            } catch (Exception e) {
              return;
            }
          }
        });
        Thread t = new Thread() {
          @Override
          public void run() {
            try {
              // if one removes this synchronized, there is a chance of deadlock
              // Reason being set.add() requires lock on Collections$SynchronizedSet
              // and SubjectDomainCombiner$WeakKeyValueMap.
              // While AccessController.checkPermission takes lock on this stuff in reverse order
              synchronized (SecurityUtil.getSubjectDomainLock(AccessController.getContext())) {
                Set<Principal> set = subject.getPrincipals();
                set.add(new Principal() {
                  @Override
                  public String getName() {
                    return "anything";
                  }
                });
                Thread.sleep(1000);
              }
            } catch (Exception e) {
            }
          }
        };
        t.start();

        Thread t1 = new Thread() {
          @Override
          public void run() {
            try {
              final FilePermission perm = new FilePermission("anything", "read");
              AccessController.checkPermission(perm);
            } catch (Exception e) {
              //
            }
          }
        };
        t1.start();
        try {
          t.join();
          t1.join();
        } catch (InterruptedException e) {
        }
        return null;
      }
    });
  }

}
