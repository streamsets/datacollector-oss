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
var EC = protractor.ExpectedConditions,
  currentNumberOfPipelines = 0;

describe('StreamSets Data Collector App', function() {

  beforeEach(function() {
    browser.ignoreSynchronization = true;
  });

  afterEach(function() {
    browser.executeScript('window.sessionStorage.clear();');
    browser.executeScript('window.localStorage.clear();');
  });

  browser.get('/');

  it('should automatically redirect to / when location fragment is empty', function() {
    expect(browser.getLocationAbsUrl()).toMatch("/");
  });


  describe('home page', function() {

    it('should render home view when user navigates to /', function() {
      browser.get('/');
      expect(browser.getTitle()).toEqual('StreamSets Data Collector');
    });

    it('should show create pipeline and import button', function() {
      browser.get('/');
      browser.sleep(1000);
      element.all(by.repeater('pipeline in pipelines')).then(function(pipelines) {
        currentNumberOfPipelines = pipelines.length;
        if(pipelines.length === 0) {
          element.all(by.css('.create-pipeline-btn-group')).then(function(elements) {
            var createBtnElement = elements[elements.length - 1];
            createBtnElement.isDisplayed().then(function (isVisible) {
              expect(isVisible).toBeTruthy();
            });
          });

          element.all(by.css('.create-pipeline-btn')).then(function(elements) {
            var createBtnElement = elements[elements.length - 1];
            createBtnElement.isDisplayed().then(function (isVisible) {
              expect(isVisible).toBeTruthy();
            });
          });

          element.all(by.css('.import-pipeline-btn')).then(function(elements) {
            var importBtnElement = elements[elements.length - 1];
            importBtnElement.isDisplayed().then(function (isVisible) {
              expect(isVisible).toBeTruthy();
            });
          });
        } else {
          element.all(by.css('.create-pipeline-btn-link')).then(function(elements) {
            var createBtnElement = elements[elements.length - 1];
            createBtnElement.isDisplayed().then(function (isVisible) {
              expect(isVisible).toBeTruthy();
            });
          });

          element.all(by.css('.import-pipeline-btn-link')).then(function(elements) {
            var importBtnElement = elements[elements.length - 1];
            importBtnElement.isDisplayed().then(function (isVisible) {
              expect(isVisible).toBeTruthy();
            });
          });
        }


      });

    });

    var pipeline1 = 'home page test'  + (new Date()).getTime();

    it('should be able to create new pipeline', function() {
      browser.get('/');
      browser.sleep(1000);

      element.all(by.repeater('pipeline in pipelines')).then(function(pipelines) {

        if(pipelines.length === 0) {
          element.all(by.css('.create-pipeline-btn')).then(function(elements) {
            var createBtnElement = elements[elements.length - 1];
            createBtnElement.click();

            element(by.model('newConfig.name')).sendKeys(pipeline1);
            element(by.model('newConfig.description')).sendKeys('pipeline description');
            element(by.css('button[type="submit"]')).click();

            browser.sleep(1500);
          });
        } else {
          element.all(by.css('.create-pipeline-btn-link')).then(function(elements) {
            var createBtnElement = elements[elements.length - 1];
            createBtnElement.click();

            element(by.model('newConfig.name')).sendKeys(pipeline1);
            element(by.model('newConfig.description')).sendKeys('pipeline description');
            element(by.css('button[type="submit"]')).click();

            browser.sleep(1500);
          });
        }
      });

      browser.get('/');
      browser.sleep(1000);
      element.all(by.repeater('pipeline in pipelines')).then(function(pipelines) {
        expect(pipelines.length).toEqual(currentNumberOfPipelines + 1);
        currentNumberOfPipelines++;
      });

    });

    it('should be able to duplicate the pipeline created above', function() {
      var pipelineElement;

      browser.get('/');
      browser.sleep(1000);
      element.all(by.repeater('pipeline in pipelines'))
        .then(function(pipelines) {
          var deferred = protractor.promise.defer();
          pipelines.forEach(function(pipeline) {
            var tagElement = pipeline.element(by.tagName('h2'));
            tagElement.getText().then(function(text) {
              if(text == pipeline1) {
                pipelineElement = pipeline;
              }
            });
          });

          return browser.sleep(1000);
        })
        .then(function() {
          expect(pipelineElement).toBeDefined();

          //Click dropdown toggle icon
          pipelineElement.element(by.css('.dropdown-toggle')).click();

          //Click Duplicate button in dropdown
          pipelineElement.element(by.css('[ng-click="duplicatePipelineConfig(pipeline, $event)"]')).click();

          browser.sleep(500);

          element(by.css('.duplicate-modal-form')).element(by.css('button[type="submit"]')).click();

          return browser.sleep(2000);
        })
        .then(function() {
          browser.get('/');
          browser.sleep(1000);
          element.all(by.repeater('pipeline in pipelines')).then(function(pipelines) {
            expect(pipelines.length).toEqual(currentNumberOfPipelines + 1);
            currentNumberOfPipelines++;
          });
        });
    });

    it('should be able to export the pipeline created above', function() {
      var pipeline2 = pipeline1 + ' copy',
        pipelineElement;

      browser.get('/');
      browser.sleep(1000);
      element.all(by.repeater('pipeline in pipelines'))
        .then(function(pipelines) {
          var deferred = protractor.promise.defer();
          pipelines.forEach(function(pipeline) {
            var tagElement = pipeline.element(by.tagName('h2'));
            tagElement.getText().then(function(text) {
              if(text == pipeline2) {
                pipelineElement = pipeline;
              }
            });
          });

          return browser.sleep(1000);
        })
        .then(function() {
          expect(pipelineElement).toBeDefined();

          //Click dropdown toggle icon
          pipelineElement.element(by.css('.dropdown-toggle')).click();

          //Click Export button in dropdown
          pipelineElement.element(by.css('[ng-click="exportPipelineConfig(pipeline, $event)"]')).click();

        });
    });


    it('should be able to import the pipeline', function() {
      var pipeline2 = pipeline1 + ' copy',
        pipelineElement;

      browser.get('/');
      browser.sleep(1000);
      element.all(by.repeater('pipeline in pipelines'))
        .then(function(pipelines) {
          var deferred = protractor.promise.defer();
          pipelines.forEach(function(pipeline) {
            var tagElement = pipeline.element(by.tagName('h2'));
            tagElement.getText().then(function(text) {
              if(text == pipeline2) {
                pipelineElement = pipeline;
              }
            });
          });

          return browser.sleep(1000);
        })
        .then(function() {
          expect(pipelineElement).toBeDefined();

          //Click dropdown toggle icon
          pipelineElement.element(by.css('.dropdown-toggle')).click();

          //Click Export button in dropdown
          pipelineElement.element(by.css('[ng-click="importPipelineConfig(pipeline, $event)"]')).click();

          element(by.css('input[type="file"]')).sendKeys(__dirname + '/testData/testPipeline.json');

          browser.sleep(1500);

          element(by.css('button[type="submit"]')).click();

          browser.sleep(1500);

        });
    });


    it('should be able to delete the pipeline', function() {
      var pipeline2 = pipeline1 + ' copy',
        pipelineElement;

      browser.get('/');
      browser.sleep(1000);
      element.all(by.repeater('pipeline in pipelines'))
        .then(function(pipelines) {
          var deferred = protractor.promise.defer();
          pipelines.forEach(function(pipeline) {
            var tagElement = pipeline.element(by.tagName('h2'));
            tagElement.getText().then(function(text) {
              if(text == pipeline2) {
                pipelineElement = pipeline;
              }
            });
          });

          return browser.sleep(1000);
        })
        .then(function() {
          expect(pipelineElement).toBeDefined();

          //Click dropdown toggle icon
          pipelineElement.element(by.css('.dropdown-toggle')).click();

          //Click Export button in dropdown
          pipelineElement.element(by.css('[ng-click="deletePipelineConfig(pipeline, $event)"]')).click();

          //Click yes button
          element(by.css('[ng-click="yes()"]')).click();

          return browser.sleep(1000);
        })
        .then(function() {
          browser.get('/');
          browser.sleep(1000);
          element.all(by.repeater('pipeline in pipelines')).then(function(pipelines) {
            expect(pipelines.length).toEqual(currentNumberOfPipelines - 1);
            currentNumberOfPipelines--;
          });
        });
    });


  });
});
