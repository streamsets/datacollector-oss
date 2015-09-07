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

var frisby = require('frisby');


frisby.create('Trying to access /jmx before login should redirect to login page')
  .get(browser.baseUrl + 'jmx')
  .expectStatus(200)
  .expectHeaderContains('content-type', 'text/html')
  .toss();


frisby.create('Login to StreamSets Data Collector')
  .get(browser.baseUrl + 'login?j_username=admin&j_password=admin')
  .expectStatus(200)
  .expectHeader('Content-Type', 'text/html')
  .after(function(body, res) {
    var cookie = res.headers['set-cookie'];

    /**
     * GET /jmx
     */
    frisby.create('Should return JMX JSON data')
      .get(browser.baseUrl + 'jmx', {
          headers:  {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Cookie": cookie
          }
        })
      .inspectJSON()
      .expectStatus(200)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(jmxJSON) {
        expect(jmxJSON.beans).toBeDefined();
        expect(jmxJSON.beans.length > 0).toBeTruthy();
      })
      .toss();

  })
  .toss();


