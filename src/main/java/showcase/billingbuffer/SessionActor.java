package showcase.billingbuffer;

import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;
import com.rbmhtechnology.eventuate.AbstractEventsourcedActor;
import com.rbmhtechnology.eventuate.ConcurrentVersions;
import com.rbmhtechnology.eventuate.ConcurrentVersionsTree;
import com.rbmhtechnology.eventuate.ResultHandler;
import com.rbmhtechnology.eventuate.SnapshotMetadata;
import com.rbmhtechnology.eventuate.Versioned;
import com.rbmhtechnology.eventuate.VersionedAggregate;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.rbmhtechnology.eventuate.VersionedAggregate.AggregateDoesNotExistException;
import static com.rbmhtechnology.eventuate.VersionedAggregate.DomainCmd;
import static com.rbmhtechnology.eventuate.VersionedAggregate.DomainEvt;
import static com.rbmhtechnology.eventuate.VersionedAggregate.Resolve;
import static com.rbmhtechnology.eventuate.VersionedAggregate.Resolved;

public class SessionActor extends AbstractEventsourcedActor {
    private final String sessionId;
    private VersionedAggregate<Session, SessionCommand, SessionEvent> sessionAggregate;

    public SessionActor(String sessionId, String replicaId, ActorRef eventLog) {
        super(String.format("j-%s-%s", sessionId, replicaId), eventLog);
        this.sessionId = sessionId;
        this.sessionAggregate = VersionedAggregate.create(
                sessionId,
                (session, sessionCommand) -> sessionCommand.execute(replicaId, session),
                (session, sessionEvent) -> sessionEvent.apply(session),
                SessionDomainCmd.instance,
                SessionDomainEvt.instance
        );

        setOnCommand(ReceiveBuilder
                .match(CreateSession.class, c -> sessionAggregate.validateCreate(c, processCommand(sessionId, sender(), self())))
                .match(SessionCommand.class, c -> sessionAggregate.validateUpdate(c, processCommand(sessionId, sender(), self())))
                .match(Resolve.class, c -> sessionAggregate.validateResolve(c.selected(), replicaId, processCommand(sessionId, sender(), self())))
                .match(GetState.class, c -> sender().tell(createStateFromAggregate(sessionId, sessionAggregate), self()))
                .match(SaveSnapshot.class, c -> saveState(sender(), self()))
                .build());

        setOnEvent(ReceiveBuilder
                .match(SessionCreated.class, e -> {
                    sessionAggregate = sessionAggregate.handleCreated(e, lastVectorTimestamp(), lastSequenceNr());
                    if (!recovering()) printSession(sessionAggregate.getVersions());
                })
                .match(SessionEvent.class, e -> {
                    sessionAggregate = sessionAggregate.handleUpdated(e, lastVectorTimestamp(), lastSequenceNr());
                    if (!recovering()) printSession(sessionAggregate.getVersions());
                })
                .match(Resolved.class, e -> {
                    sessionAggregate = sessionAggregate.handleResolved(e, lastVectorTimestamp(), lastSequenceNr());
                    if (!recovering()) printSession(sessionAggregate.getVersions());
                })
                .build());

        setOnSnapshot(ReceiveBuilder
                .match(ConcurrentVersionsTree.class, s -> {
                    sessionAggregate = sessionAggregate.withAggregate(((ConcurrentVersionsTree<Session, SessionEvent>) s).withProjection((BiFunction<Session, SessionEvent, Session>) (session, sessionEvent) -> sessionEvent.apply(session)));
                    System.out.println(String.format("[%s] Snapshot loaded:", sessionId));
                    printSession(sessionAggregate.getVersions());
                })
                .build());

        setOnRecover(ResultHandler
                .onSuccess(v -> {
                    System.out.println(String.format("[%s] Recovery complete:", sessionId));
                    printSession(sessionAggregate.getVersions());
                }));
    }

    @Override
    public Optional<String> getAggregateId() {
        return Optional.of(sessionId);
    }

    private <E> ResultHandler<E> processCommand(String sessionId, ActorRef sender, ActorRef self) {
        return ResultHandler.on(
                evt -> processEvent(evt, sender, self),
                err -> sender.tell(new CommandFailure(sessionId, err), self)
        );
    }

    private <E> void processEvent(E event, ActorRef sender, ActorRef self) {
        persist(event, ResultHandler.on(
                evt -> sender.tell(new CommandSuccess(sessionId), self),
                err -> sender.tell(new CommandFailure(sessionId, err), self)
        ));
    }

    private void saveState(ActorRef sender, ActorRef self) {
        if (sessionAggregate.getAggregate().isPresent()) {
            save(sessionAggregate.getAggregate().get(), ResultHandler.on(
                    metadata -> sender.tell(new SaveSnapshotSuccess(sessionId, metadata), self),
                    err -> sender.tell(new SaveSnapshotFailure(sessionId, err), self)
            ));
        } else {
            sender.tell(new SaveSnapshotFailure(sessionId, new AggregateDoesNotExistException(sessionId)), self);
        }
    }

    private GetStateSuccess createStateFromAggregate(String id, VersionedAggregate<Session, SessionCommand, SessionEvent> agg) {
        return new GetStateSuccess(agg.getAggregate().map(ConcurrentVersions::getAll).map(getVersionMap(id)).orElseGet(Collections::emptyMap));
    }

    private Function<List<Versioned<Session>>, Map<String, List<Versioned<Session>>>> getVersionMap(String id) {
        return versions -> {
            HashMap<String, List<Versioned<Session>>> map = new HashMap<>();
            map.put(id, versions);
            return map;
        };
    }

    static void printSession(List<Versioned<Session>> versions) {
        if (versions.size() > 1) {
            System.out.println("Conflict:");
            IntStream.range(0, versions.size()).forEach(i -> System.out.println("- version " + i + ": " + versions.get(i).value()));
        } else if (versions.size() == 1) {
            System.out.println(versions.get(0).value());
        }
    }

    public static class SessionDomainCmd implements DomainCmd<SessionCommand> {
        public static SessionDomainCmd instance = new SessionDomainCmd();

        public String origin(SessionCommand cmd) {
            return "";
        }
    }

    public static class SessionDomainEvt implements DomainEvt<SessionEvent> {
        public static SessionDomainEvt instance = new SessionDomainEvt();

        public String origin(SessionEvent evt) {
            if (evt instanceof SessionCreated) {
                return ((SessionCreated) evt).origin;
            } else {
                return "";
            }
        }
    }
    
    public static abstract class SessionCommand<T extends SessionEvent> implements Serializable {
        public String sessionId;
        public BiFunction<String, Session, T> executor;

        protected SessionCommand(String sessionId, BiFunction<String, Session, T> executor) {
            this.sessionId = sessionId;
            this.executor = executor;
        }

        public T execute(String origin, Session session) {
            if (session != null
                && !session.sessionId.equals(sessionId))
            {
                throw new RuntimeException("session id mismatch");
            }
            return executor.apply(origin, session);
        }
    }

    public static class CreateSession extends SessionCommand<SessionCreated> {
        public CreateSession(String sessionId, String sessionIP, boolean isPrepaid) {
            super(sessionId, (origin, session) -> new SessionCreated(sessionId, origin, new Date(), sessionIP, isPrepaid));
        }
    }

    public static class DeleteSession extends SessionCommand<SessionDeleted> {
        public DeleteSession(String sessionId) {
            super(sessionId, (origin, session) -> new SessionDeleted(sessionId, origin, new Date()));
        }
    }

    public static class UpdateSessionUsage extends SessionCommand<SessionUsageUpdated> {
        public UpdateSessionUsage(String sessionId, long uploadOverall, long downloadOverall) {
            super(sessionId, (origin, session) -> new SessionUsageUpdated(sessionId, origin, new Date(), uploadOverall, downloadOverall));
        }
    }

    public static class BillSession extends SessionCommand<SessionBilled> {
        public BillSession(String sessionId, long uploadAllowed, long uploadBilled, long downloadAllowed, long downloadBilled) {
            super(sessionId, (origin, session) -> new SessionBilled(sessionId, origin, new Date(), uploadAllowed, uploadBilled, downloadAllowed, downloadBilled));
        }
    }

    public static abstract class SessionEvent implements Serializable {
        public String sessionId;
        public Date timestamp;
        public String origin;

        public SessionEvent(String sessionId, String origin, Date timestamp) {
            this.sessionId = sessionId;
            this.origin = origin;
            this.timestamp = timestamp;
        }

        public abstract Session apply(Session session);
    }

    public static class SessionCreated extends SessionEvent {
        public String sessionIP;
        public boolean isPrepaid;

        public SessionCreated(String sessionId, String origin, Date timestamp, String sessionIP, boolean isPrepaid) {
            super(sessionId, origin, timestamp);
            this.sessionIP = sessionIP;
            this.isPrepaid = isPrepaid;
        }

        @Override
        public Session apply(Session session) {
            assert session == null;
            return new Session(
                    sessionId, 
                    sessionIP, 
                    isPrepaid, 
                    0, 
                    0, 
                    0, 
                    0, 
                    0, 
                    0, 
                    timestamp, 
                    origin, 
                    null, 
                    null, 
                    null, 
                    null, 
                    null, 
                    null);
        }
    }

    public static class SessionDeleted extends SessionEvent {
        public SessionDeleted(String sessionId, String origin, Date timestamp) {
            super(sessionId, origin, timestamp);
        }

        @Override
        public Session apply(Session session) {
            assert session != null;
            return new Session(
                    sessionId,
                    session.sessionIP,
                    session.isPrepaid,
                    session.uploadOverall,
                    session.uploadAllowed,
                    session.uploadBilled,
                    session.downloadOverall,
                    session.downloadAllowed,
                    session.downloadBilled,
                    session.creationTimestamp,
                    session.creationOrigin,
                    timestamp,
                    origin,
                    session.lastUsageTimestamp,
                    session.lastUsageOrigin,
                    session.lastBillingTimestamp,
                    session.lastBillingOrigin
            );
        }
    }

    public static class SessionUsageUpdated extends SessionEvent {
        public long uploadOverall;
        public long downloadOverall;

        public SessionUsageUpdated(String sessionId, String origin, Date timestamp, long uploadOverall, long downloadOverall) {
            super(sessionId, origin, timestamp);
            this.uploadOverall = uploadOverall;
            this.downloadOverall = downloadOverall;
        }

        @Override
        public Session apply(Session session) {
            assert session != null;
            return new Session(
                    sessionId,
                    session.sessionIP,
                    session.isPrepaid,
                    uploadOverall,
                    session.uploadAllowed,
                    session.uploadBilled,
                    downloadOverall,
                    session.downloadAllowed,
                    session.downloadBilled,
                    session.creationTimestamp,
                    session.creationOrigin,
                    session.deletionTimestamp,
                    session.deletionOrigin,
                    timestamp,
                    origin,
                    session.lastBillingTimestamp,
                    session.lastBillingOrigin
            );
        }
    }

    public static class SessionBilled extends SessionEvent {
        public long uploadAllowed;
        public long uploadBilled;
        public long downloadAllowed;
        public long downloadBilled;

        public SessionBilled(String sessionId, String origin, Date timestamp, long uploadAllowed, long uploadBilled, long downloadAllowed, long downloadBilled) {
            super(sessionId, origin, timestamp);
            this.uploadAllowed = uploadAllowed;
            this.uploadBilled = uploadBilled;
            this.downloadAllowed = downloadAllowed;
            this.downloadBilled = downloadBilled;
        }

        @Override
        public Session apply(Session session) {
            assert session != null;
            return new Session(
                    sessionId,
                    session.sessionIP,
                    session.isPrepaid,
                    session.uploadOverall,
                    uploadAllowed,
                    uploadBilled,
                    session.downloadOverall,
                    downloadAllowed,
                    downloadBilled,
                    session.creationTimestamp,
                    session.creationOrigin,
                    session.deletionTimestamp,
                    session.deletionOrigin,
                    session.lastUsageTimestamp,
                    session.lastUsageOrigin,
                    timestamp,
                    origin
            );
        }
    }

    public static class GetState {
        public static GetState instance = new GetState();

        private GetState() {
        }
    }

    public static class GetStateSuccess {
        public Map<String, List<Versioned<Session>>> state;

        private GetStateSuccess(Map<String, List<Versioned<Session>>> state) {
            this.state = Collections.unmodifiableMap(state);
        }

        public static GetStateSuccess empty() {
            return new GetStateSuccess(Collections.emptyMap());
        }

        public GetStateSuccess merge(GetStateSuccess that) {
            Map<String, List<Versioned<Session>>> result = new HashMap<>();
            result.putAll(this.state);
            result.putAll(that.state);
            return new GetStateSuccess(result);
        }
    }

    public static class GetStateFailure {
        public Throwable cause;

        public GetStateFailure(Throwable cause) {
            this.cause = cause;
        }
    }

    public static class CommandSuccess implements Serializable {
        public String sessionId;

        public CommandSuccess(String sessionId) {
            this.sessionId = sessionId;
        }
    }

    public static class CommandFailure implements Serializable {
        public Throwable cause;
        public String sessionId;

        public CommandFailure(String sessionId, Throwable cause) {
            this.sessionId = sessionId;
            this.cause = cause;
        }
    }

    public static class SaveSnapshot implements Serializable {
        public String sessionId;

        public SaveSnapshot(String sessionId) {
            this.sessionId = sessionId;
        }
    }

    public static class SaveSnapshotSuccess implements Serializable {
        public SnapshotMetadata metadata;
        public String sessionId;

        public SaveSnapshotSuccess(String sessionId, SnapshotMetadata metadata) {
            this.sessionId = sessionId;
            this.metadata = metadata;
        }
    }

    public static class SaveSnapshotFailure implements Serializable {
        public Throwable cause;
        public String sessionId;

        public SaveSnapshotFailure(String sessionId, Throwable cause) {
            this.sessionId = sessionId;
            this.cause = cause;
        }
    }
}