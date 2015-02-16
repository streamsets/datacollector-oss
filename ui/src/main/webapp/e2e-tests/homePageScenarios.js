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

    it('should show create pipeline button when pipeline list is empty', function() {
      element.all(by.repeater('pipeline in pipelines')).then(function(pipelines) {
        expect(pipelines.length).toEqual(0);
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
    });

    it('should be able to import pipeline', function() {
      element.all(by.css('.import-pipeline-btn')).then(function(elements) {
        var importBtnElement = elements[elements.length - 1];
        importBtnElement.click();
        $('input[type="file"]').sendKeys(__dirname + '/testData/testPipeline.json');
        element(by.css('button[type="submit"]')).click();

        browser.sleep(500);


        //Toggle Library Pane
        element(by.css('[ng-click="toggleLibraryPanel()"]')).click();

        //Test pipeline creation by checking list of pipelines
        element.all(by.repeater('pipeline in pipelines')).then(function(pipelines) {
          expect(pipelines.length).toEqual(1);
          expect(pipelines[0].element(by.binding('pipeline.name')).getText()).toEqual('UI End to End Test Pipeline');
        });

      });
    });


    it('should be able to create new pipeline', function() {
      element(by.css('[ng-click="toggleLibraryPanel()"]')).click();

      element.all(by.css('.create-pipeline-btn')).then(function(elements) {
        var createBtnElement = elements[0];
        createBtnElement.click();

        //Fill Create Pipeline Modal Dialog values
        element(by.model('newConfig.name')).sendKeys('Sample Pipeline');
        element(by.model('newConfig.description')).sendKeys('pipeline description');
        element(by.css('button[type="submit"]')).click();

        browser.sleep(500);

        //Test pipeline creation by checking list of pipelines
        element.all(by.repeater('pipeline in pipelines')).then(function(pipelines) {
          expect(pipelines.length).toEqual(2);
          expect(pipelines[0].element(by.binding('pipeline.name')).getText()).toEqual('Sample Pipeline');
        });

      });
    });


    it('should be able to duplicate pipeline', function() {
      //Toggle Library Pane
      element(by.css('[ng-click="toggleLibraryPanel()"]')).click();

      element.all(by.repeater('pipeline in pipelines')).then(function(pipelines) {

        pipelines[0].element(by.css('.dropdown-toggle')).click();

        pipelines[0].element(by.css('[ng-click="duplicatePipelineConfig(pipeline, $event)"]')).click();


        element(by.model('newConfig.name')).sendKeys(' Duplicate Copy');
        element(by.model('newConfig.description')).sendKeys('pipeline description');
        element(by.css('button[type="submit"]')).click();

        browser.sleep(500);

        //Test pipeline creation by checking list of pipelines
        element.all(by.repeater('pipeline in pipelines')).then(function(pipelines) {
          expect(pipelines.length).toEqual(3);
          expect(pipelines[1].element(by.binding('pipeline.name')).getText()).toEqual('Sample Pipelinecopy Duplicate Copy');
        });

      });
    });


    it('should be able to delete pipeline', function() {
      //Toggle Library Pane
      element(by.css('[ng-click="toggleLibraryPanel()"]')).click();

      element.all(by.repeater('pipeline in pipelines')).then(function(pipelines) {

        //Select 2 pipeline
        pipelines[1].click();

        browser.sleep(500);

        element.all(by.repeater('pipeline in pipelines')).then(function(pipelines) {
          //Click dropdown toggle icon
          pipelines[1].element(by.css('.dropdown-toggle')).click();

          //Click Delete button in dropdown
          pipelines[1].element(by.css('[ng-click="deletePipelineConfig(pipeline, $event)"]')).click();


          //Click yes button
          element(by.css('[ng-click="yes()"]')).click();


          browser.sleep(500);

          //Test pipeline deletion by checking list of pipelines
          element.all(by.repeater('pipeline in pipelines')).then(function(pipelines) {
            expect(pipelines.length).toEqual(2);
          });
        });

      });
    });

    it('should be able to toggle stage library', function() {
      //Select Sample Pipeline

      //Toggle Library Pane
      element(by.css('[ng-click="toggleLibraryPanel()"]')).click();

      element.all(by.repeater('pipeline in pipelines')).then(function(pipelines) {


      });
    });


  });
});
