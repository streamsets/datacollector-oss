/**
 * Contact module for displaying home page content
 */

angular
  .module('pipelineAgentApp.data',[
    'ngRoute'
  ])
  .config(['$routeProvider', function($routeProvider){
    $routeProvider.when("/data",
      {
        templateUrl: "app/data/data.tpl.html",
        controller: "DataController"
      }
    );
  }])
  .controller('DataController', function($scope){

  });