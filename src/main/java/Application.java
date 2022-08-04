import cluster.management.LeaderElection;
import cluster.management.OnElectionCallback;
import cluster.management.ServiceRegistry;
import java.io.IOException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class Application implements Watcher {
  private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
  private static final int SESSION_TIMEOUT = 3000;
  private ZooKeeper zooKeeper;
  private static final int DEFAULT_PORT = 8000;

  public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
    int currentServerPort = args.length == 1 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
    Application application = new Application();
    ZooKeeper zooKeeper = application.connectToZookeeper();

    ServiceRegistry serviceRegistry = new ServiceRegistry(zooKeeper);

    OnElectionAction onElectionAction = new OnElectionAction(serviceRegistry, currentServerPort);

    LeaderElection leaderElection = new LeaderElection(zooKeeper, onElectionAction);
    leaderElection.volunteerForLeadership();
    leaderElection.reelectLeader();

    application.run();
    application.close();
    System.out.println("disconnected from zookeeper, existing application");

  }

  public ZooKeeper connectToZookeeper() throws IOException {
    this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    return zooKeeper;
  }

  public void run() throws InterruptedException {
    synchronized (zooKeeper) {
      zooKeeper.wait();
    }
  }

  public void close() throws InterruptedException {
    synchronized (zooKeeper) {
      zooKeeper.close();
    }
  }

  @Override
  public void process(WatchedEvent watchedEvent) {
    switch (watchedEvent.getType()) {
      case None:
        if (watchedEvent.getState() == KeeperState.SyncConnected) {
          System.out.println("successfully connected to Zookeeper");
        } else {
          synchronized (zooKeeper) {
            System.out.println("disconnected from Zookeeper");
            zooKeeper.notifyAll();

          }
        }
    }
  }
}
