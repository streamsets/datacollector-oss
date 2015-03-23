exports.config = {
  allScriptsTimeout: 11000,

  suites: {
    //sample: ['ui/logPage.js']
    restAPI: [
      'restAPI/infoResource.js',
      'restAPI/logoutResource.js',
      'restAPI/configurationResource.js',
      'restAPI/helpResource.js',
      'restAPI/jmx.js',
      'restAPI/stageLibraryResource.js',
      'restAPI/pipelineStoreResource.js'
    ],
    ui: [
      'ui/homePage.js',
      'ui/configurationPage.js',
      'ui/jvmMetricsPage.js',
      'ui/logPage.js'
    ]
  },

  capabilities: {
    //'browserName': 'firefox'
    'browserName': 'chrome'
  },

  //baseUrl: env.baseUrl,

  framework: 'jasmine',

  jasmineNodeOpts: {
    defaultTimeoutInterval: 30000
  },

  onPrepare: function() {
    browser.sleep(3000);

    browser.driver.get(browser.baseUrl + 'login.html');

    browser.driver.findElement(by.id('usernameId')).sendKeys('admin');
    browser.driver.findElement(by.id('passwordId')).sendKeys('admin');
    browser.driver.findElement(by.id('loginId')).click();

    // Login takes some time, so wait until it's done.
    browser.driver.wait(function() {
      return browser.driver.getCurrentUrl().then(function(url) {
        return true;
      });
    });
  }
};
