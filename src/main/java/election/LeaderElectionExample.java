package election;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class LeaderElectionExample extends LeaderSelectorListenerAdapter
{
    private final String leaderName;
    private final LeaderSelector leaderSelector;
    private int leaderCount = 0;
    private final CuratorFramework zooClient;

    public LeaderElectionExample(String zooKeeperAddress, int baseSleepTimeMs, int maxRetries, String path, String leaderName)
    {
        this.leaderName = leaderName;

        this.zooClient = CuratorFrameworkFactory.newClient(zooKeeperAddress, new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries));
        this.leaderSelector = new LeaderSelector(zooClient, path, this);

        // Set all the instances to requeue when it relinquishes leadership
        this.leaderSelector.autoRequeue();
    }

    public void start() throws IOException
    {
        this.zooClient.start();
        this.leaderSelector.start();
    }

    public void close()
    {
        try
        {
            this.leaderSelector.close();
            this.zooClient.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public void takeLeadership(CuratorFramework client) throws Exception
    {
        //This method should not return until we want to relinquish leadership
        int seconds = 3;

        System.out.println(this.leaderName + " is the leader. Waiting " + seconds + " seconds...");
        this.leaderCount ++;
        System.out.println(this.leaderName + " has been leader " + this.leaderCount + " time(s) before.");
        try
        {
            Thread.sleep(seconds * 1000);
        }
        catch ( InterruptedException e )
        {
            System.err.println(this.leaderName + " was interrupted.");
            Thread.currentThread().interrupt();
        }
        finally
        {
            System.out.println(this.leaderName + " gives up the leadership.\n");
        }
    }

    public static void main (String[] args)
    {
        int clientNumber = 10;
        String zooKeeperAddress = "192.168.56.101:2181,192.168.56.101:2182,192.168.56.101:2183";
        int baseSleepTimeMs = 1000;
        int maxRetries = 3;
        int clientNum = 10;
        String zookeeperPath = "/School/LeaderElection";

        List<LeaderElectionExample> leaderClients = new ArrayList<LeaderElectionExample>();

        try
        {
            //init the clients
            for (int i = 0; i < clientNumber; i ++)
            {
                LeaderElectionExample leaderClient = new LeaderElectionExample(zooKeeperAddress, baseSleepTimeMs, maxRetries, zookeeperPath, "Client[" + i + "]");
                leaderClient.start();
                leaderClients.add(leaderClient);
            }
            System.out.println("Press enter/return to quit\n");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            System.out.println("Shutting down...");

            for (LeaderElectionExample leaderClient : leaderClients)
            {
                leaderClient.close();
            }
        }
    }
}
