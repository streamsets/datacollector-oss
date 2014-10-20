describe('pipelineAgentApp module', function() {
  var $rootScope, $scope, $controller;

  beforeEach(module('pipelineAgentApp'));

  beforeEach(inject(function(_$rootScope_, _$controller_){
    $rootScope = _$rootScope_;
    $scope = $rootScope.$new();
    $controller = _$controller_;
  }));

  it('should have correct page title.', function() {
    expect($rootScope.common.title == 'StreamSets | Data In Motion').toBeTruthy();
  });

});
