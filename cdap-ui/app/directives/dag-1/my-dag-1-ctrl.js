function Ctrl (Redux, MyDagStore) {
  MyDagStore.subscribe(() => {
    this.store = MyDagStore.getState();
  });
  MyDagStore.dispatch({
    type: 'ADD',
    nodeType: 'transform',
    cssClass: 'transform',
    icon: 'Script'
  });
  MyDagStore.dispatch({
    type: 'ADD',
    nodeType: 'transform',
    cssClass: 'transform',
    icon: 'Script'
  });
}
Ctrl.$inject = ['Redux', 'MyDagStore'];
angular.module(PKG.name + '.commons')
  .controller('MyDag1Ctrl', Ctrl);
