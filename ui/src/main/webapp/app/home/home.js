/**
 * Home module for displaying home page content
 */

angular
  .module('pipelineAgentApp.home',[
    'ngRoute',
    'bgDirectives'
  ])
  .config(['$routeProvider', function($routeProvider){
    $routeProvider.when("/",
      {
        templateUrl: "app/home/home.tpl.html",
        controller: "HomeController"
      }
    );
  }])
  .controller('HomeController', function($scope, api){
    api.pipelineAgent.getConfig().success(function (res) {
      $scope.config = res;
      console.log($scope.config);
    });


    $scope.createPipelineAgent = function() {
      $scope.config = {
        source: {
          name: 'File Source'
        }
      };
    };

  });