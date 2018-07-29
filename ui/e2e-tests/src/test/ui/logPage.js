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
describe('StreamSets Data Collector App', function() {

  beforeEach(function() {
    browser.ignoreSynchronization = true;
    //browser.manage().timeouts().pageLoadTimeout(10000);
  });

  afterEach(function() {
    browser.executeScript('window.sessionStorage.clear();');
    browser.executeScript('window.localStorage.clear();');
  });

  describe('Log page', function() {

    beforeEach(function() {
      browser.get('/collector/logs');
    });

    it('should render log page view when user navigates to /collector/logs', function() {
      browser.sleep(1000);
      element.all(by.repeater('logMessage in logMessages'))
        .then(function(logMessages) {
          expect(logMessages.length > 0).toBeTruthy();
        });
    });

    it('should be able filter the logs', function() {
      element(by.css('.severity-dropdown .btn')).click().then(function() {
        element(by.css('[ng-click="severityFilterChanged(\'INFO\');"]')).click().then(function() {
          browser.sleep(1000);
          element.all(by.repeater('logMessage in logMessages'))
            .then(function(logMessages) {
              expect(logMessages.length > 0).toBeTruthy();
            });
        });
      });
    });

  });
});
