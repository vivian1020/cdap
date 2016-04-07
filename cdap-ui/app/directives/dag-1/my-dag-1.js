angular.module(PKG.name + '.commons')
  .directive('myDag1', () => {
    return {
      restrict: 'E',
      templateUrl: 'dag-1/my-dag-1.html',
      controller: 'MyDag1Ctrl',
      controllerAs: 'MyDag1Ctrl',
      link: function(scope, element) {
        scope.element = element;
        scope.dagContainer = document.getElementById('dag-container');
        scope.diagramContainer = document.getElementById('diagram-container');
      }
    };
  });
