import cluster.management.LeaderElection;
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
  public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
    Application application = new Application();
    ZooKeeper zooKeeper = application.connectToZookeeper();

    LeaderElection leaderElection = new LeaderElection(zooKeeper);
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
