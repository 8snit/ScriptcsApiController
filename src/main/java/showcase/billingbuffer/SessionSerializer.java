package showcase.billingbuffer;

import akka.actor.ExtendedActorSystem;
import akka.serialization.JSerializer;
import akka.serialization.SerializationExtension;
import javaslang.collection.List;

import java.io.Serializable;

public class SessionSerializer extends JSerializer {
    private ExtendedActorSystem system;

    public SessionSerializer(ExtendedActorSystem system) {
        this.system = system;
    }

    @Override
    public int identifier() {
        return 10001;
    }

    @Override
    public boolean includeManifest() {
        return true;
    }

    @Override
    public Object fromBinaryJava(byte[] bytes, Class<?> manifest) {
        return SerializationExtension.get(system).deserialize(bytes, Session.class).get();
    }

    @Override
    public byte[] toBinary(Object o) {
        return SerializationExtension.get(system).serialize(o).get();
    }
}
