function Dag1TestCtrl(MyDagStore, $timeout) {
  this.addNode = () => {
    MyDagStore.dispatch({
      type: 'ADD',
      nodeType: 'transform',
      cssClass: 'transform',
      icon: 'Script',
      name: this.nodename
    });
  };
}
Dag1TestCtrl.$inject = ['MyDagStore', '$timeout'];

angular.module(`${PKG.name}.feature.experimental`)
  .controller('Dag1TestCtrl', Dag1TestCtrl);
