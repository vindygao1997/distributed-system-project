package cluster.management;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ServiceRegistry implements Watcher {
  private static final String REGISTRY_ZNODE= "/service_registry";
  private final ZooKeeper zooKeeper;
  private String currentZnode = null;
  private List<String> allServiceAddresses = null;

  public ServiceRegistry(ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;
    createServiceRegistryZnode();
  }

  public void registerToCluster(String metadata) throws InterruptedException, KeeperException {
    // metadata can be anything we want to share with the cluster, here it's the address of the znode
    this.currentZnode = zooKeeper.create(REGISTRY_ZNODE + "/n_", metadata.getBytes(),
        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    System.out.println("registered to service registry");
  }

  public synchronized List<String> getAllServiceAddresses()
      throws InterruptedException, KeeperException {
    if (allServiceAddresses == null) {
      updateAddresses();
    }
    return allServiceAddresses;
  }

  public void unregisterFromCluster() throws InterruptedException, KeeperException {
    if (currentZnode != null && zooKeeper.exists(currentZnode, false) != null) {
      zooKeeper.delete(currentZnode, -1);
    }
  }

  /**
   * A method for worker zNodes to register for updates in addresses stored in Service Registry
   */
  public void registerForUpdates() {
    try {
      updateAddresses();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (KeeperException e) {
      e.printStackTrace();
    }
  }

  private void createServiceRegistryZnode() {
    // if registry node does not exist
    try {
      if (zooKeeper.exists(REGISTRY_ZNODE, false) == null) {
        zooKeeper.create(REGISTRY_ZNODE, new byte[]{}, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    } catch (KeeperException e) {
      // if two nodes create registry node at the same time, the second node will trigger this exception
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * A function that obtains the most up-to-date list of addresses of all worker zNodes under the
   * Service Registry
   * @throws InterruptedException
   * @throws KeeperException
   */
  private synchronized void updateAddresses() throws InterruptedException, KeeperException {
    // 1. get children 2. register to see if any changes happen
    List<String> workerZnodes = zooKeeper.getChildren(REGISTRY_ZNODE, this);
    List<String> addresses = new ArrayList<>(workerZnodes.size());

    for (String workerZnode : workerZnodes) {
      String workerZnodeFullPath = REGISTRY_ZNODE + "/" + workerZnode;
      Stat stat = zooKeeper.exists(workerZnodeFullPath, false); // prerequisite for getting znode info
      // if the znode disappear between get children and exists, will return null
      if(stat == null) {
        continue;
      }
      byte[] addressBytes = zooKeeper.getData(workerZnodeFullPath, false, stat);
      String address = new String(addressBytes);
      addresses.add(address);
    }
    this.allServiceAddresses = Collections.unmodifiableList(addresses);
    System.out.println("the cluster addresses are: " + this.allServiceAddresses);
  }


  @Override
  public void process(WatchedEvent watchedEvent) {
    try {
      updateAddresses();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (KeeperException e) {
      e.printStackTrace();
    }
  }
}
