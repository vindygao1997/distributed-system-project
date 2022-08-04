package cluster.management;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.Leader;

// threading model:
// IO thread: connection:
// Event thread: handles all events from zookeeper server

// to be a watcher for the zookeeper, this class needs to implement watcher interface
public class LeaderElection implements Watcher {
  private static final String ELECTION_NAMESPACE = "/election";
  private ZooKeeper zooKeeper;
  private String currentZNodeName;
  private final OnElectionCallback onElectionCallback;

  public LeaderElection(ZooKeeper zooKeeper, OnElectionCallback onElectionCallback) {

    this.zooKeeper = zooKeeper;
    this.onElectionCallback = onElectionCallback;
  }

  public void volunteerForLeadership() throws InterruptedException, KeeperException {
    String zNodePrefix = ELECTION_NAMESPACE + "/c_"; // create path to store zNodes under /election/c_, c_means candidates
    String zNodeFullPath = zooKeeper.create(zNodePrefix, new byte[]{}, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL); // create new znode sequentially, which gives each of them a number
    this.currentZNodeName = zNodeFullPath.replace(ELECTION_NAMESPACE + "/", ""); // extract its name and store locally
  }

  public void reelectLeader() throws InterruptedException, KeeperException {
    String predecessorNodeName = "";
    Stat predecessorStat = null;
    // consistently find available predecessor zNode until finding one
    // since there will surely be a first node, so we don't need to handle out of bound exception
    while (predecessorStat == null) {
      List<String> children = this.zooKeeper.getChildren(ELECTION_NAMESPACE, false);

      Collections.sort(children); // sort it so that the smallest number will be the first one
      String firstZNode = children.get(0);

      if (firstZNode.equals(currentZNodeName)) {
        // this is the leader
        System.out.println("im the leader");
        onElectionCallback.onElectedToBeLeader();
        return;
      } else {
        // if not the leader, get the node before it
        System.out.println("im NOT the leader");
        int predecessorIndex = Collections.binarySearch(children, currentZNodeName) - 1;
        predecessorNodeName = children.get(predecessorIndex);
        predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorNodeName, this);
      }
    }
    onElectionCallback.onWorker();
    System.out.println("watching node " + predecessorNodeName);
    System.out.println();
  }

  // event thread: will be called when an event comes from zookeeper
  @Override
  public void process(WatchedEvent watchedEvent) {
    switch(watchedEvent.getType()) {
      case NodeDeleted:
        try {
          reelectLeader();
        } catch (InterruptedException e) {
        } catch (KeeperException e) {
        }
    }
  }



}
