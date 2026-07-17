package digitaltwin;

import java.io.Serializable;

// lo manda Node-RED cuando detecta que un nodo ha dejado de dar señales de vida
public record NodeCrashMsg(String nodeId) implements Serializable {
}
