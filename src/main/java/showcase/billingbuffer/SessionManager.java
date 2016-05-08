package showcase.billingbuffer;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.dispatch.Futures;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.Patterns;
import com.rbmhtechnology.eventuate.AbstractEventsourcedView;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.Future;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.rbmhtechnology.eventuate.VersionedAggregate.Resolve;
import static showcase.billingbuffer.SessionActor.GetState;
import static showcase.billingbuffer.SessionActor.GetStateFailure;
import static showcase.billingbuffer.SessionActor.GetStateSuccess;
import static showcase.billingbuffer.SessionActor.SessionCommand;
import static showcase.billingbuffer.SessionActor.SessionCreated;
import static showcase.billingbuffer.SessionActor.SaveSnapshot;
import static scala.compat.java8.JFunction.func;
import static scala.compat.java8.JFunction.proc;

public class SessionManager extends AbstractEventsourcedView {

    private final String replicaId;
    private Map<String, ActorRef> sessionActors;

    public SessionManager(String replicaId, ActorRef eventLog) {
        super(String.format("j-om-%s", replicaId), eventLog);
        this.replicaId = replicaId;
        this.sessionActors = HashMap.empty();

        setOnCommand(ReceiveBuilder
                .match(SessionCommand.class, c -> sessionActor(c.sessionId).tell(c, sender()))
                .match(SaveSnapshot.class, c -> sessionActor(c.sessionId).tell(c, sender()))
                .match(Resolve.class, c -> sessionActor(c.id()).tell(c, sender()))
                .match(GetState.class, c -> sessionActors.isEmpty(), c -> replyStateZero(sender()))
                .match(GetState.class, c -> !sessionActors.isEmpty(), c -> replyState(sender()))
                .build());

        setOnEvent(ReceiveBuilder
                .match(SessionCreated.class, e -> !sessionActors.containsKey(e.sessionId), e -> sessionActor(e.sessionId))
                .build());
    }

    private ActorRef sessionActor(final String sessionId) {
        return sessionActors.get(sessionId)
                .getOrElse(() -> {
                    final ActorRef sessionActor = context().actorOf(Props.create(SessionActor.class, sessionId, replicaId, eventLog()));
                    sessionActors = sessionActors.put(sessionId, sessionActor);
                    return sessionActor;
                });
    }

    private void replyStateZero(ActorRef target) {
        target.tell(GetStateSuccess.empty(), self());
    }

    private void replyState(ActorRef target) {
        final ExecutionContextExecutor dispatcher = context().dispatcher();

        Futures.sequence(getActorStates()::iterator, dispatcher)
                .map(func(this::toStateSuccess), dispatcher)
                .onComplete(proc(result -> {
                    if (result.isSuccess()) {
                        target.tell(result.get(), self());
                    } else {
                        target.tell(new GetStateFailure(result.failed().get()), self());
                    }
                }), dispatcher);
    }

    private Stream<Future<GetStateSuccess>> getActorStates() {
        return sessionActors.values().toJavaStream().map(this::asyncGetState);
    }

    private Future<GetStateSuccess> asyncGetState(final ActorRef actor) {
        return Patterns.ask(actor, GetState.instance, 10000L).map(func(o -> (GetStateSuccess) o), context().dispatcher());
    }

    private GetStateSuccess toStateSuccess(final Iterable<GetStateSuccess> states) {
        return StreamSupport.stream(states.spliterator(), false).reduce(GetStateSuccess::merge).get();
    }
}
