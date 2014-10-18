/**
 * Service for providing access to the backend API via HTTP.
 */

angular.module('pipelineAgentApp.common').factory('api', function($rootScope, $http) {
  var apiBase = '/api',
    api = {events: {}};

  //api http endpoints
  api.pipelineAgent = {
    getConfig: function() {
      var url = apiBase + '/config';
      return $http({
        method: 'GET',
        url: url
      });
    }
  };

  return api;
});