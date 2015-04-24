describe('Controller: modules/home/HomeCtrl', function () {
  var $rootScope, $scope, $controller, $httpBackend, mockedApi;

  beforeEach(module('dataCollectorApp'));

  beforeEach(inject(function (_$rootScope_, _$controller_, _$httpBackend_, api, _) {
    $rootScope = _$rootScope_;
    $scope = $rootScope.$new();
    $controller = _$controller_;
    mockedApi = api;
    $controller('HomeController', {
      '$rootScope': $rootScope,
      '$scope': $scope,
      'api': mockedApi,
      '_': _
    });
  }));

});