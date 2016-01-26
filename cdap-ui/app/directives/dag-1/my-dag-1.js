angular.module(PKG.name + '.commons')
  .directive('myDag1', () => {
    return {
      restrict: 'E',
      templateUrl: 'dag-1/my-dag-1.html',
      controller: 'MyDag1Ctrl',
      controllerAs: 'MyDag1Ctrl'
    };
  });
