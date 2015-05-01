/**
 * Object limitTo filter
 */

angular.module('dataCollectorApp.filters')
  .filter('objLimitTo', function() {
    return function(obj, limit){
      var keys = Object.keys(obj);
      if(keys.length < 1){
        return [];
      }
      var ret = {},
        count = 0;

      keys.sort();

      angular.forEach(keys, function(key, arrayIndex){
        if(count >= limit){
          return false;
        }
        ret[key] = obj[key];
        count++;
      });

      return ret;
    };
  });