package digitaltwin;

// lo manda Node-RED cuando el nodo vuelve a dar señales de vida tras un crash
public record NodeRecoveredMsg(String nodeId) {
}
