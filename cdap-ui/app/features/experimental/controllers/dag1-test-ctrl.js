function Dag1TestCtrl(MyDagStore) {
  this.addNode = () => {
    MyDagStore.dispatch({
      name: this.nodename,
      cssClass: this.cssClass,
      icon: this.icon,
      endpoint: this.endpoints,
      type: 'ADD-NODE'
    });
  };
  MyDagStore.subscribe(() => {
    this.state = MyDagStore.getState();
  });
}
Dag1TestCtrl.$inject = ['MyDagStore'];

angular.module(`${PKG.name}.feature.experimental`)
  .controller('Dag1TestCtrl', Dag1TestCtrl);
