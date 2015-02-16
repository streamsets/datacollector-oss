var env = {
  baseUrl: 'http://localhost:18630/'
};

exports.config = {
  allScriptsTimeout: 11000,

  specs: [
    '*.js'
  ],

  capabilities: {
    //'browserName': 'firefox'
    'browserName': 'chrome'
  },

  baseUrl: env.baseUrl,

  framework: 'jasmine',

  jasmineNodeOpts: {
    defaultTimeoutInterval: 30000
  },

  onPrepare: function() {
    browser.driver.get(env.baseUrl + 'login.html');

    browser.driver.findElement(by.id('usernameId')).sendKeys('admin');
    browser.driver.findElement(by.id('passwordId')).sendKeys('admin');
    browser.driver.findElement(by.id('loginId')).click();

    // Login takes some time, so wait until it's done.
    // For the test app's login, we know it's done when it redirects to
    // index.html.
    browser.driver.wait(function() {
      return browser.driver.getCurrentUrl().then(function(url) {
        //return /index/.test(url);
        return true;
      });
    });
  }
};
