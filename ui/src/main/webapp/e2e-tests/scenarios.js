describe('StreamSets Data Collector App', function() {

  beforeEach(function() {
    browser.ignoreSynchronization = true;
    browser.manage().timeouts().pageLoadTimeout(10000);
  });

  browser.get('/');

  it('should automatically redirect to / when location fragment is empty', function() {
    expect(browser.getLocationAbsUrl()).toMatch("/");
  });


  describe('home page', function() {

    beforeEach(function() {
      browser.get('/');
    });

    it('should render home view when user navigates to /', function() {
      expect(browser.getTitle()).toEqual('StreamSets Data Collector');
    });

    it('should show create pipeline button when no pipeline list is empty', function() {
      element.all(by.repeater('pipeline in pipelines')).then(function(pipelines) {
        expect(pipelines.length).toEqual(0);
      });

      element.all(by.css('.create-pipeline-btn')).then(function(elements) {
        var createBtnElement = elements[elements.length - 1];
        createBtnElement.isDisplayed().then(function (isVisible) {
          expect(isVisible).toBeTruthy();
        });
      });
    });


    it('should be able to create new pipeline', function() {
      element.all(by.css('.create-pipeline-btn')).then(function(elements) {
        var createBtnElement = elements[elements.length - 1];
        createBtnElement.click();

        //Fill Create Pipeline Modal Dialog values
        element(by.model('newConfig.name')).sendKeys('samplePipeline');
        element(by.model('newConfig.description')).sendKeys('pipeline description');
        element(by.css('button[type="submit"]')).click();


        browser.sleep(500);

        //Toggle Library Pane
        element(by.css('[ng-click="toggleLibraryPanel()"]')).click();

        //Test pipeline creation by checking list of pipelines
        element.all(by.repeater('pipeline in pipelines')).then(function(pipelines) {
          expect(pipelines.length).toEqual(1);
          expect(pipelines[0].element(by.binding('pipeline.name')).getText()).toEqual('samplePipeline');
        });

      });
    });





  });
});
