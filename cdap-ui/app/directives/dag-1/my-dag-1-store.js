let _uuid;
let nodesReducer = (state = [], action = {}) => {
  switch(action.type) {
    case 'ADD':
      return [
        ...state,
        {
          id: _uuid.v4(),
          type: action.nodeType,
          icon: action.icon,
          cssClass: action.cssClass,
          name: action.name
        }
      ];
    case 'REMOVE':
      return state.filter(node => node.id === action.id);
    default:
      return state;
  }
};

let Store = (Redux, uuid) => {
  _uuid = uuid;
  console.log('store');
  return Redux.createStore(nodesReducer);
};
Store.$inject = ['Redux', 'uuid'];

angular.module(PKG.name + '.commons')
  .factory('MyDagStore', Store);
