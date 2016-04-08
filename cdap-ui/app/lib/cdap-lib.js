angular.module(PKG.name+'.commons')
  .factory('Redux', function($window) {
    return $window.Redux;
  })
  .factory('Undoable', function($window) {
    return $window.undoable;
  });
