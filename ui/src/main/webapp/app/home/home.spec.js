describe('Controller: modules/home/HomeCtrl', function() {
  var $rootScope, $scope, $controller, $httpBackend, mockedApi;

  beforeEach(module('pipelineAgentApp'));

  beforeEach(inject(function(_$rootScope_, _$controller_, _$httpBackend_, api){
    $rootScope = _$rootScope_;
    $scope = $rootScope.$new();
    $controller = _$controller_;
    $httpBackend = _$httpBackend_;
    mockedApi = api;
  }));

  it('should make home menu item active.', function() {
    $controller('HomeController', {
      '$rootScope' : $rootScope,
      '$scope': $scope,
      'api' : mockedApi
    });

    expect($rootScope.common.active.home == 'active');
  });

  it('should call the getConfig api function', function() {

    $httpBackend.expectGET('api/config').respond(undefined);

    $controller('HomeController', {
      '$rootScope' : $rootScope,
      '$scope': $scope,
      'api' : mockedApi
    });

    //expect($scope.config).toBeUndefined();

  });

});