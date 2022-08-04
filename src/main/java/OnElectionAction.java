import cluster.management.OnElectionCallback;
import cluster.management.ServiceRegistry;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.zookeeper.KeeperException;

public class OnElectionAction implements OnElectionCallback {

  private final ServiceRegistry serviceRegistry;
  private final int port;

  public OnElectionAction(ServiceRegistry serviceRegistry, int port) {
    this.serviceRegistry = serviceRegistry;
    this.port = port;
  }

  @Override
  public void onElectedToBeLeader() {
    try {
      serviceRegistry.unregisterFromCluster();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (KeeperException e) {
      e.printStackTrace();
    }
    serviceRegistry.registerForUpdates();
  }

  @Override
  public void onWorker() {
    String currentServerAddress = null;
    try {
      currentServerAddress = String.format("http://%s:%d", InetAddress.getLocalHost().getCanonicalHostName(), port);
      serviceRegistry.registerToCluster(currentServerAddress);
    } catch (UnknownHostException | InterruptedException | KeeperException e) {
      e.printStackTrace();
    }
  }
}
