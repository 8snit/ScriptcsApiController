package showcase.billingbuffer;

import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;
import com.rbmhtechnology.eventuate.AbstractEventsourcedView;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import scala.Console;

import java.io.Serializable;

public class SessionView extends AbstractEventsourcedView {
    private Map<String, Integer> updateCounts;

    public SessionView(String replicaId, ActorRef eventLog) {
        super(String.format("j-ov-%s", replicaId), eventLog);
        this.updateCounts = HashMap.empty();

        setOnCommand(ReceiveBuilder
                .match(GetUpdateCount.class, this::handleGetUpdateCount)
                .build());

        setOnEvent(ReceiveBuilder
                .match(SessionActor.SessionEvent.class, this::handleSessionEvent)
                .build());
    }

    public void handleGetUpdateCount(final GetUpdateCount cmd) {
        final String sessionId = cmd.sessionId;
        Console.out().println(sessionId + "guc view");
        sender().tell(new GetUpdateCountSuccess(sessionId, updateCounts.get(sessionId).getOrElse(0)), self());
    }

    public void handleSessionEvent(final SessionActor.SessionEvent evt) {
        final String id = evt.sessionId;
        Console.out().println(id + "oe view");
        updateCounts = updateCounts.put(id, updateCounts.get(id).map(cnt -> cnt + 1).getOrElse(1));
    }

    // ------------------------------------------------------------------------------
    //  Domain commands
    // ------------------------------------------------------------------------------

    public static class GetUpdateCount implements Serializable {
        public final String sessionId;

        public GetUpdateCount(String sessionId) {
            this.sessionId = sessionId;
        }
    }

    public static class GetUpdateCountSuccess implements Serializable {
        public final int count;
        public final String sessionId;

        public GetUpdateCountSuccess(String sessionId, int count) {
            this.sessionId = sessionId;
            this.count = count;
        }
    }
}
