/**
 * Authorization Service
 */

angular.module('dataCollectorApp.common')
  .constant('userRoles', {
    admin: 'admin',
    creator: 'creator',
    manager: 'manager',
    guest: 'guest'
  })
  .service('authService', function($rootScope, $q, api) {
    var self = this;

    this.initializeDefer = undefined;

    this.init = function() {
      if(!self.initializeDefer) {
        self.initializeDefer = $q.defer();

        $q.all([
          api.admin.getUserInfo()
        ])
          .then(function (results) {
            self.userInfo = results[0].data;
            self.initializeDefer.resolve();
          }, function(data) {
            self.initializeDefer.reject(data);
          });
      }

      return self.initializeDefer.promise;
    };

    this.isAuthorized = function (authorizedRoles) {
      if (!angular.isArray(authorizedRoles)) {
        authorizedRoles = [authorizedRoles];
      }

      var intersection = _.intersection(self.userInfo.roles, authorizedRoles);

      return intersection && intersection.length;
    };

    this.getUserName = function() {
      return self.userInfo ? self.userInfo.user: '';
    };
  });