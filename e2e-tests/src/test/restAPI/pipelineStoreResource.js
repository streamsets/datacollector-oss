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
    var cookie = res.headers['set-cookie'],
      initialPipelineCount = 0;

    /**
     * GET rest/v1/pipelines
     */
    frisby.create('Should return all Pipeline Configuration Info.')
      .get(browser.baseUrl + 'rest/v1/pipelines', {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie
        }
      })
      .inspectJSON()
      .expectStatus(200)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(pipelineListJSON) {
        expect(pipelineListJSON).toBeDefined();

        //expect(pipelineListJSON.length).toEqual(0);
        initialPipelineCount = pipelineListJSON.length;
      })
      .toss();


    /**
     * PUT rest/v1/pipeline/<PIPELINE_NAME>?description=<DESCRIPTION>
     */
    frisby.create('Should be able to save new pipeline.')
      .put(browser.baseUrl + 'rest/v1/pipeline/pipeline1?description=pipeline%20description', {}, {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie,
          "X-Requested-By": "CSRF"
        }
      })
      .inspectJSON()
      .expectStatus(201)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(pipelineJSON) {
        expect(pipelineJSON).toBeDefined();
        expect(pipelineJSON.info.name).toEqual('pipeline1');
        expect(pipelineJSON.info.description).toEqual('pipeline description');
      })
      .toss();

    /**
     * GET rest/v1/pipeline
     */
    frisby.create('Should return newly saved Pipeline Configuration Info.')
      .get(browser.baseUrl + 'rest/v1/pipelines', {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie
        }
      })
      .inspectJSON()
      .expectStatus(200)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(pipelineListJSON) {
        expect(pipelineListJSON).toBeDefined();
        expect(pipelineListJSON.length).toEqual(initialPipelineCount + 1);

        var found = false;
        for(var i in pipelineListJSON) {
          var pipeline = pipelineListJSON[i];
          if(pipeline.name === 'pipeline1') {
            found = true;
          }
        }

        expect(found).toEqual(true);
      })
      .toss();

    /**
     * PUT rest/v1/pipeline/<PIPELINE_NAME>?description=<DESCRIPTION>
     */
    frisby.create('Should throw exception when trying to save pipeline with duplicate name.')
      .put(browser.baseUrl + 'rest/v1/pipeline/pipeline1?description=pipeline%20description', {}, {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie,
          "X-Requested-By": "CSRF"
        }
      })
      .inspectJSON()
      .expectStatus(500)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(errorJSON) {
        expect(errorJSON).toBeDefined();
        expect(errorJSON.RemoteException).toBeDefined();
        expect(errorJSON.RemoteException.errorCode).toEqual('CONTAINER_0201');
      })
      .toss();


    /**
     * GET rest/v1/pipeline/<PIPELINE_NAME>?get=info
     */
    frisby.create('Should be able to fetch pipeline info.')
      .get(browser.baseUrl + 'rest/v1/pipeline/pipeline1?get=info', {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie,
          "X-Requested-By": "CSRF"
        }
      })
      .inspectJSON()
      .expectStatus(200)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(pipelineInfoJSON) {
        expect(pipelineInfoJSON).toBeDefined();
        expect(pipelineInfoJSON.name).toEqual('pipeline1');
        expect(pipelineInfoJSON.description).toEqual('pipeline description');
        expect(pipelineInfoJSON.creator).toEqual('admin');
        expect(pipelineInfoJSON.lastModifier).toEqual('admin');
        expect(pipelineInfoJSON.valid).toEqual(false);
      })
      .toss();

    /**
     * GET rest/v1/pipeline/<PIPELINE_NAME>
     */
    frisby.create('Should be able to fetch pipeline configuration.')
      .get(browser.baseUrl + 'rest/v1/pipeline/pipeline1', {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie
        }
      })
      .inspectJSON()
      .expectStatus(200)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(pipelineJSON) {
        var pipeline1ConfigJSON = pipelineJSON;

        expect(pipelineJSON).toBeDefined();
        expect(pipelineJSON.info.name).toEqual('pipeline1');
        expect(pipelineJSON.info.description).toEqual('pipeline description');
        expect(pipelineJSON.info.creator).toEqual('admin');
        expect(pipelineJSON.info.lastModifier).toEqual('admin');
        expect(pipelineJSON.info.valid).toEqual(false);



        expect(pipelineJSON.schemaVersion).toEqual(1);
        expect(pipelineJSON.uuid).toBeDefined();
        expect(pipelineJSON.configuration.length > 2).toBeTruthy();
        expect(pipelineJSON.stages.length).toEqual(0);
        expect(pipelineJSON.errorStage).toEqual(null);


        expect(pipelineJSON.issues.pipelineIssues.length).toEqual(2);
        expect(pipelineJSON.issues.pipelineIssues[0].message).toEqual('VALIDATION_0001 - The pipeline is empty');
        expect(pipelineJSON.issues.pipelineIssues[1].configGroup).toEqual('BAD_RECORDS');
        expect(pipelineJSON.issues.pipelineIssues[1].configName).toEqual('badRecordsHandling');


        expect(pipelineJSON.previewable).toEqual(false);
        expect(pipelineJSON.valid).toEqual(false);


        /*
        //Update description and try to save it.
        pipeline1ConfigJSON.description = 'Changed pipeline description';

        console.log(pipeline1ConfigJSON);

        frisby.create('Should be save pipeline configuration.')
          .post(browser.baseUrl + 'rest/v1/pipeline/pipeline1',
            pipeline1ConfigJSON,
            {
              json: true,
              headers:  {
                "Content-Type": "application/json;charset=UTF-8",
                "Accept": "application/json",
                "Cookie": cookie
              }
            }
          )
          .inspectJSON()
          .expectStatus(200)
          .expectHeaderContains('content-type', 'application/json')
          .afterJSON(function(pipelineJSON) {
            pipeline1ConfigJSON = pipelineJSON;

            expect(pipelineJSON).toBeDefined();
            expect(pipelineJSON.info.name).toEqual('pipeline1');
            expect(pipelineJSON.info.description).toEqual('Changed pipeline description');
            expect(pipelineJSON.info.creator).toEqual('admin');
            expect(pipelineJSON.info.lastModifier).toEqual('admin');
            expect(pipelineJSON.info.valid).toEqual(false);

            expect(pipelineJSON.schemaVersion).toEqual(1);
            expect(pipelineJSON.uuid).toBeDefined();
            expect(pipelineJSON.configuration.length).toEqual(2);
            expect(pipelineJSON.stages.length).toEqual(0);
            expect(pipelineJSON.errorStage).toEqual(null);

            expect(pipelineJSON.issues.pipelineIssues.length).toEqual(2);
            expect(pipelineJSON.issues.pipelineIssues[0].message).toEqual('VALIDATION_0001 - The pipeline is empty');
            expect(pipelineJSON.issues.pipelineIssues[1].configGroup).toEqual('BAD_RECORDS');
            expect(pipelineJSON.issues.pipelineIssues[1].configName).toEqual('badRecordsHandling');


            expect(pipelineJSON.previewable).toEqual(false);
            expect(pipelineJSON.valid).toEqual(false);
          })
          .toss();


          */

      })
      .toss();


    /**
     * GET rest/v1/pipeline/<PIPELINE_NAME>/rules
     */
    frisby.create('Should be able to fetch pipeline rules.')
      .get(browser.baseUrl + 'rest/v1/pipeline/pipeline1/rules', {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie
        }
      })
      .inspectJSON()
      .expectStatus(200)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(pipelineRulesJSON) {
        expect(pipelineRulesJSON).toBeDefined();
        expect(pipelineRulesJSON.metricsRuleDefinitions).toBeDefined();
        expect(pipelineRulesJSON.dataRuleDefinitions).toBeDefined();
        expect(pipelineRulesJSON.emailIds).toBeDefined();
        expect(pipelineRulesJSON.emailIds.length).toEqual(0);
        expect(pipelineRulesJSON.uuid).toBeDefined();
        expect(pipelineRulesJSON.ruleIssues).toBeDefined();
        expect(pipelineRulesJSON.ruleIssues.length).toEqual(0);

        //TODO: Save Pipeline Rules
      })
      .toss();

    /**
     * DELETE rest/v1/pipeline/<PIPELINE_NAME>
     */
    frisby.create('Should be able to delete pipeline configuration.')
      .delete(browser.baseUrl + 'rest/v1/pipeline/pipeline1', {}, {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie,
          "X-Requested-By": "CSRF"
        }
      })
      .expectStatus(200)
      .toss();

  })
  .toss();