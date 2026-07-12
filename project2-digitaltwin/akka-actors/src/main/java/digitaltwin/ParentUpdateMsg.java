package digitaltwin;

// lo manda Node-RED al actor cuando el nodo real cambia de padre en el
// arbol RPL
public record ParentUpdateMsg(String nodeId, String newParentId) {
}
