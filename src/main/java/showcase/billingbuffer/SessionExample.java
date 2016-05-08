package showcase.billingbuffer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.regex.*;

import akka.actor.*;
import akka.japi.pf.ReceiveBuilder;

import akka.pattern.Patterns;
import com.rbmhtechnology.eventuate.ReplicationConnection;
import com.rbmhtechnology.eventuate.ReplicationEndpoint;
import com.rbmhtechnology.eventuate.VersionedAggregate.*;
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import scala.Console;
import scala.concurrent.duration.FiniteDuration;

import static showcase.billingbuffer.SessionActor.*;
import static showcase.billingbuffer.SessionView.*;
import static scala.compat.java8.JFunction.func;
import static scala.compat.java8.JFunction.proc;

public class SessionExample extends AbstractActor {
    private static Pattern pExit = Pattern.compile("^exit\\s*");
    private static Pattern pState = Pattern.compile("^state\\s*");
    private static Pattern pCount = Pattern.compile("^count\\s+(\\w+)\\s*");
    private static Pattern pCreate = Pattern.compile("^create\\s+(\\w+)\\s*");
    private static Pattern pDelete = Pattern.compile("^delete\\s+(\\w+)\\s*");
    private static Pattern pSave = Pattern.compile("^save\\s+(\\w+)\\s*");
    private static Pattern pUpdateUsage = Pattern.compile("^updateusage\\s+(\\w+)\\s+(\\w+)\\s*");
    private static Pattern pBill = Pattern.compile("^bill\\s+(\\w+)\\s+(\\w+)\\s*");
    private static Pattern pResolve = Pattern.compile("^resolve\\s+(\\w+)\\s+(\\d+)\\s*");

    private ActorRef manager;
    private ActorRef view;

    private BufferedReader reader;

    public SessionExample(ActorRef manager, ActorRef view) {
        this.manager = manager;
        this.view = view;

        this.reader = new BufferedReader(new InputStreamReader(System.in));

        receive(ReceiveBuilder
                .match(GetStateSuccess.class, r -> {
                    r.state.values().stream().forEach(SessionActor::printSession);
                    prompt();
                })
                .match(GetStateFailure.class, r -> {
                    System.out.println(r.cause.getMessage());
                    prompt();
                })
                .match(SaveSnapshotSuccess.class, r -> {
                    System.out.println(String.format("[%s] saved snapshot at sequence number %d", r.sessionId, r.metadata.sequenceNr()));
                    prompt();
                })
                .match(SaveSnapshotFailure.class, r -> {
                    System.out.println(String.format("[%s] saved snapshot failed: %s", r.sessionId, r.cause));
                    prompt();
                })
                .match(GetUpdateCountSuccess.class, r -> {
                    System.out.println("[" + r.sessionId + "]" + " update count = " + r.count);
                    prompt();
                })
                .match(CommandFailure.class, r -> r.cause instanceof ConflictDetectedException, r -> {
                    ConflictDetectedException cause = (ConflictDetectedException) r.cause;
                    System.out.println(cause.getMessage() + ", select one of the following versions to resolve conflict");
                    printSession(cause.getVersions());
                    prompt();
                })
                .match(CommandFailure.class, r -> {
                    System.out.println(r.cause.getMessage());
                    prompt();
                })
                .match(CommandSuccess.class, r -> prompt())
                .match(String.class, this::process).build());
    }

    private void prompt() throws IOException {
        String line = reader.readLine();
        if (line != null)
            self().tell(line, null);
    }

    private void process(String cmd) throws IOException {
        // okay, a bit eager, but anyway ...
        Matcher mExit = pExit.matcher(cmd);
        Matcher mState = pState.matcher(cmd);
        Matcher mCount = pCount.matcher(cmd);
        Matcher mCreate = pCreate.matcher(cmd);
        Matcher mDelete = pDelete.matcher(cmd);
        Matcher mSave = pSave.matcher(cmd);
        Matcher mUpdateUsage = pUpdateUsage.matcher(cmd);
        Matcher mBill = pBill.matcher(cmd);
        Matcher mResolve = pResolve.matcher(cmd);

        if (mExit.matches()) {
            getContext().system().terminate();
        } else if (mState.matches()) {
            manager.tell(GetState.instance, self());
        } else if (mCount.matches()) {
            view.tell(new GetUpdateCount(mCount.group(1)), self());
        } else if (mCreate.matches()) {
            manager.tell(new CreateSession(mCreate.group(1), "0.0.0.0", true), self());
        } else if (mDelete.matches()) {
            manager.tell(new DeleteSession(mDelete.group(1)), self());
        } else if (mSave.matches()) {
            manager.tell(new SaveSnapshot(mSave.group(1)), self());
        } else if (mUpdateUsage.matches()) {
            manager.tell(new UpdateSessionUsage(mUpdateUsage.group(1), Long.parseLong(mUpdateUsage.group(2)), 0), self());
        } else if (mBill.matches()) {
            manager.tell(new BillSession(mBill.group(1), Long.parseLong(mBill.group(2)), 0, 0, 0), self());
        } else if (mResolve.matches()) {
            manager.tell(new Resolve(mResolve.group(1), Integer.parseInt(mResolve.group(2)), ""), self());
        } else {
            System.out.println("unknown command: " + cmd);
            prompt();
        }
    }

    @Override
    public void preStart() throws Exception {
        prompt();
    }

    @Override
    public void postStop() throws Exception {
        reader.close();
    }

    public static void main(String[] args) {
        if (args.length == 0)
            throw new UnsupportedOperationException("missing config file");

        File configFile = new File(args[0]);
        if (!configFile.exists())
            throw new UnsupportedOperationException("config file not found");

        Config config = ConfigFactory.parseFile(configFile);
        ActorSystem system = ActorSystem.create(ReplicationConnection.DefaultRemoteSystemName(), config);
        ReplicationEndpoint endpoint = ReplicationEndpoint.create(id -> LeveldbEventLog.props(id, "j", true), system);

        ActorRef view = system.actorOf(Props.create(SessionView.class, endpoint.id(), endpoint.logs().apply(ReplicationEndpoint.DefaultLogName())));
        if (!args[1].contains("view")) {
            ActorRef manager = system.actorOf(Props.create(SessionManager.class, endpoint.id(), endpoint.logs().apply(ReplicationEndpoint.DefaultLogName())));
            ActorRef driver = system.actorOf(Props.create(SessionExample.class, manager, view));

//            manager.tell(new CreateSession("a" + ((Integer) new Random().nextInt(10000)).toString()), ActorRef.noSender());
//            Patterns.ask(manager, new CreateSession("b" + ((Integer) new Random().nextInt(10000)).toString()), Duration.ofSeconds(5).toMillis())
//                    .onComplete(proc(result -> {
//                        if (result.isSuccess()) {
//                            manager.tell(new UseSession(((CommandSuccess) result.get()).sessionId,
//                                    String.format("Somewhere %d", new Random().nextInt(10))), ActorRef.noSender());
//                        } else {
//                            result.failed().get().printStackTrace();
//                        }
//                    }), system.dispatcher());
        }
        else
        {
            system.scheduler().schedule(
                    new FiniteDuration(1000, TimeUnit.MILLISECONDS),
                    new FiniteDuration(1000, TimeUnit.MILLISECONDS),
                    () -> {
                        Patterns.ask(view, new GetUpdateCount("a"), Duration.ofSeconds(5).toMillis())
                                .onComplete(proc(result -> {
                                    if (result.isSuccess()) {
                                        GetUpdateCountSuccess x = (GetUpdateCountSuccess) result.get();
                                        Console.out().println(x.count + "->" + x.sessionId);
                                    } else {
                                        result.failed().get().printStackTrace();
                                    }
                                }), system.dispatcher());
                    },
                    system.dispatcher());

        }

        endpoint.activate();
    }
}
