exports.config = {
  allScriptsTimeout: 11000,

  suites: {
    //sample: ['restAPI/logoutResource.js']
    restAPI: [
      'restAPI/adminResource.js',
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

  framework: 'jasmine',

  jasmineNodeOpts: {
    defaultTimeoutInterval: 30000
  },

  onPrepare: function() {
    browser.sleep(3000);
    browser.driver.manage().window().setSize(1400, 750);
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
