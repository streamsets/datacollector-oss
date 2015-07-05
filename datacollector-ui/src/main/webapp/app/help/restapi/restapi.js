/**
 * SDC RESTful API module.
 */

angular
  .module('dataCollectorApp.restapi')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider.when('/collector/restapi',
      {
        templateUrl: 'app/help/restapi/restapi.tpl.html',
        controller: 'RESTfulAPIController',
        resolve: {
          myVar: function(authService) {
            return authService.init();
          }
        },
        data: {
          authorizedRoles: ['admin', 'creator', 'manager', 'guest']
        },
        reloadOnSearch: false
      }
    );
  }])
  .controller('RESTfulAPIController', function ($scope, $rootScope, $q, configuration, Analytics, $http, _) {
    $scope.swaggerURL = '/rest/swagger.json';

    $q.all([
      configuration.init()
    ]).then(function(results) {
      if(configuration.isAnalyticsEnabled()) {
        Analytics.trackPage('/collector/restapi');
      }
    });
  });