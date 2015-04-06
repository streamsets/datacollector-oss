/**
 * Unit tests for Controller - SDCDirectoriesModalInstanceController
 */

describe('Controller: SDCDirectoriesModalInstanceController', function() {

  var $rootScope, $scope, $controller, api,
    mockSDCDirectories = {
      runtimeDir: "/Users/madhu/Documents/projects/datacollector/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT",
      configDir: "/Users/madhu/Documents/projects/datacollector/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc",
      dataDir: "/Users/madhu/Documents/projects/datacollector/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/data",
      logDir: "/Users/madhu/Documents/projects/datacollector/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/log",
      staticWebDir: "/Users/madhu/Documents/projects/datacollector/ui/target/dist"
    };

  beforeEach(module('dataCollectorApp'));

  beforeEach(inject(function(_$rootScope_, _$controller_, _api_){
    $rootScope = _$rootScope_;
    $scope = $rootScope.$new();
    $controller = _$controller_;
    api = _api_;

    spyOn(api.admin, 'getSDCDirectories').and.callFake(function() {
      return {
        then: function(callback) {
          return callback({
            data: mockSDCDirectories
          });
        }
      };
    });

    $controller('SDCDirectoriesModalInstanceController', {
      $rootScope : $rootScope,
      $scope: $scope,
      $modalInstance: {},
      api: api
    });
  }));

  it('should call service method getSDCDirectories', function() {
    expect(api.admin.getSDCDirectories).toHaveBeenCalled();
  });

  it('should set the value sdcDirectories', function() {
    expect($scope.sdcDirectories).toEqual(mockSDCDirectories);
  });

});