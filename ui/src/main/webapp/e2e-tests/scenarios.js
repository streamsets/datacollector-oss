describe('My app', function() {

  browser.get('/');

  it('should automatically redirect to / when location fragment is empty', function() {
    expect(browser.getLocationAbsUrl()).toMatch("/");
  });


  describe('home page', function() {

    beforeEach(function() {
      browser.get('/');
    });


    it('should render home view when user navigates to /', function() {
      expect(element.all(by.css('[ng-view] p')).first().getText()).
        toMatch(/Use this document as a way to quickly start any new project./);
    });

  });
});
