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
exports.config = {
  allScriptsTimeout: 11000,

  suites: {
    clean: ['clean/clean.js'],
    restAPI: [
      'restAPI/adminResource.js',
      'restAPI/infoResource.js',
      'restAPI/logoutResource.js',
      'restAPI/configurationResource.js',
      'restAPI/jmx.js',
      'restAPI/stageLibraryResource.js',
      'restAPI/pipelineStoreResource.js'
    ],
    ui: [
      'ui/pipelineHomePage.js',
      'ui/homePage.js',
      'ui/createPipeline.js',
      'ui/previewTest.js',
      'ui/configurationPage.js',
      'ui/jvmMetricsPage.js',
      'ui/logPage.js'
    ],
    single: [
      'restAPI/createTestPipelines.js'
    ]
  },

  framework: 'jasmine',

  jasmineNodeOpts: {
    defaultTimeoutInterval: 30000
  },

  capabilities: {
    'phantomjs.binary.path': require('phantomjs').path,
    'phantomjs.ghostdriver.cli.args': ['--loglevel=DEBUG']
  },

  onPrepare: function() {
    browser.sleep(3000);
    browser.driver.manage().window().setSize(1400, 750);
    browser.ignoreSynchronization = true;
    browser.driver.get(browser.baseUrl + 'login.html');

    browser.driver.findElement(by.id('usernameId')).sendKeys('admin');
    browser.driver.findElement(by.id('passwordId')).sendKeys('admin');
    browser.driver.findElement(by.id('loginId')).click();

    browser.getCapabilities().then(function (cap) {
      browser.browserName = cap.caps_.browserName;
    });

    // Login takes some time, so wait until it's done.
    return browser.driver.wait(function() {
      return browser.driver.getCurrentUrl().then(function(url) {
        return true;
      });
    });
  }
};
