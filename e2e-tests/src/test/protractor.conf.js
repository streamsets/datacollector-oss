exports.config = {
  allScriptsTimeout: 11000,

  suites: {
    clean: ['clean/clean.js'],
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
      'ui/pipelineHomePage.js',
      'ui/homePage.js',
      'ui/createPipeline.js',
      'ui/previewTest.js',
      'ui/configurationPage.js',
      'ui/jvmMetricsPage.js',
      'ui/logPage.js'
    ],
    single: [
      'ui/previewTest.js'
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
