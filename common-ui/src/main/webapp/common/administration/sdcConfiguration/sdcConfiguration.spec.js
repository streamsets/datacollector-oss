describe('Controller: modules/sdcConfiguration/SDCConfigurationController', function () {

  beforeEach(module('dataCollectorApp.sdcConfiguration'));

  var $controller, $q, $scope, configuration, Analytics;

  beforeEach(inject(function(_$controller_, _$q_) {
    $controller = _$controller_;
    $q = _$q_;
    $scope = {};
    configuration = {
      init: function() {
       return $q.when({});
      }
    };
    Analytics = {};

  }));

  it('calls configuration init method', function() {
    spyOn(configuration, 'init');

    var controller = $controller('SDCConfigurationController', {
      $scope: $scope,
      $rootScope: {},
      $q: $q,
      Analytics: Analytics,
      configuration: configuration,
      _: _
    });

    expect(configuration.init).toHaveBeenCalled();

  });

  it('calls Analytics trackPage method with url /collector/configuration', function() {
    configuration = {
      init: function() {
        var deferred = $q.defer();
        deferred.resolve();
        return deferred.promise;
      },
      isAnalyticsEnabled: function() {
        return true;
      }
    };

    Analytics = {
      trackPage: function(url) {

      }
    };

    spyOn(configuration, 'init');
    spyOn(Analytics, 'trackPage');

    var controller = $controller('SDCConfigurationController', {
      $scope: $scope,
      $rootScope: {},
      $q: $q,
      Analytics: Analytics,
      configuration: configuration,
      _: _
    });

    expect($scope.initDefer).toBeDefined();

    $scope.initDefer.then(function() {
      expect(configuration.init).toHaveBeenCalled();
      expect(Analytics.trackPage).toHaveBeenCalledWith('/collector/configuration');
    });

  });

});