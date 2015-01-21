/**
 * Service for broadcasting whether browser is visible or not using HTML5 Page Visibility API.
 */
angular.module('pipelineAgentApp.common')
  .service('visibilityBroadcaster', function($rootScope, $document, _) {

    var document = $document[0],
      features,
      detectedFeature;

    features = {
      standard: {
        eventName: 'visibilitychange',
        propertyName: 'hidden'
      },
      moz: {
        eventName: 'mozvisibilitychange',
        propertyName: 'mozHidden'
      },
      ms: {
        eventName: 'msvisibilitychange',
        propertyName: 'msHidden'
      },
      webkit: {
        eventName: 'webkitvisibilitychange',
        propertyName: 'webkitHidden'
      }
    };

    Object.keys(features).some(function(feature) {
      if (_.isBoolean(document[features[feature].propertyName])) {
        detectedFeature = features[feature];
        return true;
      }
    });

    // Feature doesn't exist in browser.
    if (!detectedFeature) {
      return;
    }

    $document.on(detectedFeature.eventName, broadcastChangeEvent);

    function broadcastChangeEvent() {
      $rootScope.$broadcast('visibilityChange',
        document[detectedFeature.propertyName]);
    }

  });