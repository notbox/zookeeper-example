package election;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LeaderLatchExample
{
    private List<CuratorFramework> clients;
    private List<LeaderLatch> members;
    private String zooKeeperAddress = "";
    private int baseSleepTimeMs = 0;
    private int maxRetries = 0;
    private int clientNum = 0;
    //Leader Zookeeper path
    private String path = "/School/LeaderLatch";


    public LeaderLatchExample(String zooKeeperAddress, int baseSleepTimeMs, int maxRetries, int clientNum)
    {
        this.zooKeeperAddress = zooKeeperAddress;
        this.baseSleepTimeMs = baseSleepTimeMs;
        this.maxRetries = maxRetries;
        this.clientNum = clientNum;

        members = new ArrayList<LeaderLatch>();
        clients = new ArrayList<CuratorFramework>();

        try
        {
            init();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        System.out.println("We wait 10 seconds for leader election...");
        sleep(10);
        System.out.println("--------------------------------------");

    }

    private void init() throws Exception
    {
        for(int i = 0; i < this.clientNum; i ++)
        {
            CuratorFramework zooClient = CuratorFrameworkFactory.newClient(zooKeeperAddress, new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries));
            clients.add(zooClient);
            LeaderLatch leaderLatch = new LeaderLatch(zooClient, path, "Client[" + i + "]");
            members.add(leaderLatch);
            zooClient.start();
            leaderLatch.start();
        }
    }

    //Find the current Leader and print it
    public void findTheLeader()
    {
        Iterator<LeaderLatch> iter;

        while (members.size() > 0)
        {
            iter = members.iterator();
            while (iter.hasNext())
            {
                LeaderLatch leaderLatch = iter.next();

                if(leaderLatch.hasLeadership())
                {
                    //Find the leader
                    System.out.println(leaderLatch.getId() + " is leader now");
                    //Close the leader
                    closeTheLeader(leaderLatch);
                    System.out.println(leaderLatch.getId() + " is removed from leader election");
                    System.out.println("--------------------------------------");
                    //Remove the leader from the list
                    iter.remove();

                    if(members.size() > 0)
                    {
                        //wait for 3 second;
                        System.out.println("Wait 3 seconds for new leader...");
                        sleep(3);
                    }
                }
            }
        }
        System.out.println("No More leader candidates in the list.");
    }

    //Close all the Zookeeper connections
    public void closeConnections()
    {
        if(this.clients != null && this.clients.size() > 0)
        {
            for(CuratorFramework client : clients)
            {
                client.close();
            }
        }
    }

    //Close the leaderLatch
    private void closeTheLeader(LeaderLatch leaderLatch)
    {
        try
        {
            if (leaderLatch != null)
            {
                leaderLatch.close();
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    private void sleep(int second)
    {
        try
        {
            Thread.sleep(second * 1000);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    public static void main(String[] args)
    {
        //Zookeeper cluster
        String zooKeeperAddress = "192.168.56.101:2181,192.168.56.101:2182,192.168.56.101:2183";
        int baseSleepTimeMs = 1000;
        int maxRetries = 3;
        int clientNum = 10;

        LeaderLatchExample example = new LeaderLatchExample(zooKeeperAddress, baseSleepTimeMs, maxRetries, clientNum);

        example.findTheLeader();
        System.out.println("Closing all the ZooKeeper connections");
        example.closeConnections();
    }
}
