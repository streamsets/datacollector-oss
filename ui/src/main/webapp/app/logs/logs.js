/**
 * Logs module for displaying logs page content.
 */

angular
  .module('pipelineAgentApp.logs')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider.when('/logs',
      {
        templateUrl: 'app/logs/logs.tpl.html',
        controller: 'LogsController'
      }
    );
  }])
  .controller('LogsController', function ($scope) {

  });