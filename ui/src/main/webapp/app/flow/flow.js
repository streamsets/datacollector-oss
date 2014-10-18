/**
 * Flow module for monitoring pipeline.
 */

angular
  .module('pipelineAgentApp.flow',[
    'ngRoute'
  ])
  .config(['$routeProvider', function($routeProvider){
    $routeProvider.when("/flow",
      {
        templateUrl: "app/flow/flow.tpl.html",
        controller: "FlowController"
      }
    );
  }])
  .controller('FlowController', function($scope){
     console.log('change');
  });