function Ctrl (Redux, MyDagStore) {
  MyDagStore.subscribe(() => {
    this.store = MyDagStore.getState();
  });
}
Ctrl.$inject = ['Redux', 'MyDagStore'];
angular.module(PKG.name + '.commons')
  .controller('MyDag1Ctrl', Ctrl);
