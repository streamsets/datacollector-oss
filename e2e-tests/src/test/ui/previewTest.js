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
var frisby = require('frisby'),
  EC = protractor.ExpectedConditions,
  pipelineName = 'Preview Pipeline' + (new Date()).getTime();

describe('StreamSets Data Collector App', function() {

  beforeEach(function() {
    browser.ignoreSynchronization = true;
  });

  afterEach(function() {
    browser.executeScript('window.sessionStorage.clear();');
    browser.executeScript('window.localStorage.clear();');
  });

  browser.get('/');

  describe('Preview Pipeline', function() {

    beforeEach(function() {
      browser.get('/');
    });

    it('should be able to preview pipeline while creating pipeline', function() {

      browser.sleep(1000);

      element.all(by.repeater('pipeline in pipelines')).then(function(pipelines) {


        if(pipelines.length === 0) {
          element.all(by.css('.create-pipeline-btn')).then(function(elements) {
            var createBtnElement = elements[elements.length - 1];
            createBtnElement.click();

            element(by.model('newConfig.name')).sendKeys(pipelineName);
            element(by.model('newConfig.description')).sendKeys('pipeline description');
            element(by.css('button[type="submit"]')).click();

            browser.sleep(1500);
          });
        } else {
          element.all(by.css('.create-pipeline-btn-link')).then(function(elements) {
            var createBtnElement = elements[elements.length - 1];
            createBtnElement.click();

            element(by.model('newConfig.name')).sendKeys(pipelineName);
            element(by.model('newConfig.description')).sendKeys('pipeline description');
            element(by.css('button[type="submit"]')).click();

            browser.sleep(1500);
          });
        }
      });


      var detailsTab = element(by.css('.detail-tabs-left'));
      browser.wait(EC.presenceOf(detailsTab), 10000);


      //Set Pipeline Configuration

      //Change description
      element(by.model('pipelineConfig.description')).sendKeys(' updated description');

      var configGroupTabs, constantsFormGroup;
      element.all(by.repeater('groupNameToLabelMap in configGroupTabs'))
        .then(function(_configGroupTabs) {
          configGroupTabs = _configGroupTabs;
          return configGroupTabs[0].element(by.tagName('a')).click();
        })
        .then(function() {
          //Select Constants Tab
          constantsFormGroup = detailsTab.element(by.css('.config_constants'));
          return constantsFormGroup.element(by.css('.btn-default')).click();
        })
        .then(function() {
          return constantsFormGroup.all(by.repeater('mapObject in detailPaneConfig.configuration[configIndex].value track by $index'));
        })
        .then(function(constants) {

          //Add one constant
          constants[0].element(by.model('mapObject.key')).sendKeys('CONSTANTA');
          return browser.executeScript(function() {
            var constantDom = document.getElementsByClassName('config_constants')[0],
              firstConstantValueDom = constantDom.getElementsByClassName('CodeMirror')[0];

            firstConstantValueDom.CodeMirror.setValue('10');
          });
        })
        .then(function() {
          //Click on Errors Group Tab
          return configGroupTabs[1].element(by.tagName('a')).click();
        })
        .then(function() {
          //Select Discard option
          return element(by.cssContainingText('option', 'Discard (Library: Basic)')).click();
        });

      //Add Source
      var selectSourceSelectElement = element(by.model('selectedSource.selected')),
        selectProcessorSelectElement,
        selectTargetSelectElement,
        dataGenConfigs,
        trList;

      selectSourceSelectElement.element(by.cssContainingText('option', 'Dev Data Generator - Dev (for development only)')).click()
        .then(function() {
          //Add Field1 to the list of fields
          detailsTab = element(by.css('.detail-tabs-left'));
          dataGenConfigs = detailsTab.element(by.css('.config_dataGenConfigs'));

          browser.wait(EC.presenceOf(dataGenConfigs), 10000);

          return dataGenConfigs.all(by.tagName('tr'))
            .then(function(trList) {
              return trList[1].element(by.cssContainingText('option', 'STRING')).click().then(function() {
                return trList[1].element(by.css('.fa-plus')).click().then(function() {
                  return dataGenConfigs.all(by.tagName('tr'))
                });
              });
            })
            .then(function(trList) {
              return trList[2].element(by.cssContainingText('option', 'INTEGER')).click().then(function() {
                return trList[2].element(by.css('.fa-plus')).click().then(function () {
                  return dataGenConfigs.all(by.tagName('tr'))
                });
              });
            })
            .then(function(trList) {
              return trList[3].element(by.cssContainingText('option', 'LONG')).click().then(function() {
                return trList[3].element(by.css('.fa-plus')).click().then(function () {
                  return dataGenConfigs.all(by.tagName('tr'))
                });
              });
            })
            .then(function(trList) {
              return trList[4].element(by.cssContainingText('option', 'FLOAT')).click().then(function() {
                return trList[4].element(by.css('.fa-plus')).click().then(function () {
                  return dataGenConfigs.all(by.tagName('tr'))
                });
              });
            })
            .then(function(trList) {
              return trList[5].element(by.cssContainingText('option', 'DOUBLE')).click().then(function() {
                return trList[5].element(by.css('.fa-plus')).click().then(function () {
                  return dataGenConfigs.all(by.tagName('tr'))
                });
              });
            })
            .then(function(trList) {
              return trList[6].element(by.cssContainingText('option', 'DATE')).click().then(function() {
                return trList[6].element(by.css('.fa-plus')).click().then(function () {
                  return dataGenConfigs.all(by.tagName('tr'))
                });
              });
            })
            .then(function(trList) {
              return trList[7].element(by.cssContainingText('option', 'BOOLEAN')).click().then(function() {
                return trList[7].element(by.css('.fa-plus')).click().then(function () {
                  return dataGenConfigs.all(by.tagName('tr'))
                });
              });
            })
            .then(function(trList) {
              return trList[8].element(by.cssContainingText('option', 'BYTE_ARRAY')).click();
            })
            .then(function() {
              return browser.executeScript(function() {
                var dataGenConfigsDom = document.getElementsByClassName('config_dataGenConfigs')[0],
                  trList  = dataGenConfigsDom.getElementsByTagName('tr');
                trList[1].getElementsByClassName('CodeMirror')[0].CodeMirror.setValue('stringField');
                trList[2].getElementsByClassName('CodeMirror')[0].CodeMirror.setValue('integerField');
                trList[3].getElementsByClassName('CodeMirror')[0].CodeMirror.setValue('longField');
                trList[4].getElementsByClassName('CodeMirror')[0].CodeMirror.setValue('floatField');
                trList[5].getElementsByClassName('CodeMirror')[0].CodeMirror.setValue('doubleField');
                trList[6].getElementsByClassName('CodeMirror')[0].CodeMirror.setValue('dateField');
                trList[7].getElementsByClassName('CodeMirror')[0].CodeMirror.setValue('booleanField');
                trList[8].getElementsByClassName('CodeMirror')[0].CodeMirror.setValue('byteArrayField');
              });
            });
        })
        .then(function() {
          //Try to preview pipeline after adding origin
          browser.sleep(1500);
          browser.wait(EC.presenceOf(element(by.css('select[name="newPipelineProcessor"]'))), 10000);
          return element.all(by.css('.glyphicon-eye-open'));
        })
        .then(function(elements) {
          var previewBtnElement = elements[elements.length - 1];
          expect(previewBtnElement.isPresent()).toBeTruthy();
          return previewBtnElement.click();
        })
        .then(function() {
          browser.sleep(1000);
          var runPreviewButton = element(by.css('[ng-click="runPreview()"]'));
          browser.wait(EC.presenceOf(runPreviewButton), 10000);
          return runPreviewButton.click();
        })
        .then(function() {
          expect(element(by.css('.glyphicon-eye-close')).isPresent()).toBeTruthy();
          var previewTable = element(by.css('.preview-table'));
          //browser.wait(EC.visibilityOf(previewTable), 10000);
          expect(previewTable.isPresent()).toBeTruthy();

          browser.sleep(1000);

          return previewTable.all(by.repeater('record in stagePreviewData.output')).then(function(records) {
            //expect(records.length).toEqual(10);
          });

        })
        .then(function() {
          browser.sleep(1000);
          return element(by.css('.glyphicon-eye-close')).click();
        })
        .then(function() {
          browser.wait(EC.presenceOf(element(by.css('select[name="newPipelineProcessor"]'))), 10000);
          selectProcessorSelectElement = element(by.css('select[name="newPipelineProcessor"]'));
          return selectProcessorSelectElement.element(by.cssContainingText('option', 'Dev Random Error - Dev (for development only)')).click()
        })
        .then(function() {
          browser.wait(EC.presenceOf(element(by.css('select[name="newPipelineProcessor"]'))), 10000);
          return selectProcessorSelectElement.element(by.cssContainingText('option', 'Dev Record Creator - Dev (for development only)')).click()
        })
        .then(function() {
          browser.wait(EC.presenceOf(element(by.css('select[name="newPipelineTarget"]'))), 10000);
          selectTargetSelectElement = element(by.css('select[name="newPipelineTarget"]'));
          return selectTargetSelectElement.element(by.cssContainingText('option', 'Trash - Basic')).click()
        })
        .then(function() {
          //Try to preview pipeline after processor and target origin
          browser.sleep(1500);
          browser.wait(EC.presenceOf(element(by.css('.glyphicon-eye-open'))), 10000);
          return element.all(by.css('.glyphicon-eye-open'));
        })
        .then(function(elements) {
          var previewBtnElement = elements[elements.length - 1];
          expect(previewBtnElement.isPresent()).toBeTruthy();
          return previewBtnElement.click();
        })
        .then(function() {
          browser.sleep(1000);
          var runPreviewButton = element(by.css('[ng-click="runPreview()"]'));
          browser.wait(EC.presenceOf(runPreviewButton), 10000);
          return runPreviewButton.click();
        })
        .then(function() {
          expect(element(by.css('.glyphicon-eye-close')).isPresent()).toBeTruthy();
          var previewTable = element(by.css('.preview-table'));
          //browser.wait(EC.visibilityOf(previewTable), 10000);
          browser.sleep(1500);

          return previewTable.all(by.repeater('record in stagePreviewData.output')).then(function(records) {
            //expect(records.length).toEqual(10);
          });

        })
        .then(function() {
          //Click on next stage to check second stage preview data

          var nextStageBtn = element(by.css('[ng-click="nextStageInstances.length === 0 || nextStagePreview(nextStageInstances[0], stagePreviewData.output)"]'));
          browser.wait(EC.presenceOf(nextStageBtn), 10000);
          return nextStageBtn.click();
        })
        .then(function() {
          //Check the input and output of second stage preview data
          expect(element(by.css('.preview-table')).isPresent()).toBeTruthy();
        });

    });

  });


});
