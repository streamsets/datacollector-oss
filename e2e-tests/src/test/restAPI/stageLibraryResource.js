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
var frisby = require('frisby');

frisby.create('Login to StreamSets Data Collector')
  .get(browser.baseUrl + 'login?j_username=admin&j_password=admin')
  .expectStatus(200)
  .expectHeader('Content-Type', 'text/html')
  .after(function(body, res) {
    var cookie = res.headers['set-cookie'];

    /**
     * GET rest/v1/definitions
     */
    frisby.create('Should return Pipeline & Stage Library definitions')
      .get(browser.baseUrl + 'rest/v1/definitions', {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie
        }
      })
      //.inspectJSON()
      .expectStatus(200)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(definitionsJSON) {
        expect(definitionsJSON).toBeDefined();

        expect(definitionsJSON.pipeline).toBeDefined();
        expect(definitionsJSON.pipeline.length === 1).toBeTruthy();

        expect(definitionsJSON.stages).toBeDefined();
        expect(definitionsJSON.stages.length > 1).toBeTruthy();


        var firstStageDefinition = definitionsJSON.stages[0],
          iconURL = browser.baseUrl + 'rest/v1/definitions/stages/' + firstStageDefinition.library + '/' +
            firstStageDefinition.name + '/icon',
          icon = firstStageDefinition.icon,
          contentType = icon.indexOf('.png') !== -1 ? 'image/png' : 'image/svg+xml';

        /**
         * GET rest/v1/definitions/stages/<LIBRARY_NAME>/<STAGE_NAME>/icon
         */
        frisby.create('Should return icon for Stage Library')
          .get(iconURL, {
            headers:  {
              "Cookie": cookie
            }
          })
          .expectStatus(200)
          .expectHeaderContains('content-type', contentType)
          .toss();

      })
      .toss();

  })
  .toss();