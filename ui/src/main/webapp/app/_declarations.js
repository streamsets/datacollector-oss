/**
 * Main Module
 */

angular.module('dataCollectorApp', [
  'ngRoute',
  'ngCookies',
  'tmh.dynamicLocale',
  'pascalprecht.translate',
  'templates-app',
  'templates-common',
  'dataCollectorApp.common',
  'dataCollectorApp.home',
  'dataCollectorApp.jvmMetrics',
  'dataCollectorApp.logs',
  'ngStorage'
]);