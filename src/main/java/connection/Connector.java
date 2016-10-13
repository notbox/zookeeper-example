package connection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;


public class Connector
{
    public static void main(String[] args)
    {
        //Zookeeper cluster
        //String zooKeeperAddress = "";
        String zooKeeperAddress = "192.168.56.101:2181,192.168.56.101:2182,192.168.56.101:2183";
        int baseSleepTimeMs = 1000;
        int maxRetries = 3;
        String testPath = "/testConnection";
        String nameSpace = "School";

        //Create Zookeeper connection
        //Connection without namespace
        //CuratorFramework connector =  CuratorFrameworkFactory.newClient(zooKeeperAddress, new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries));
        //Connection with namespace
        CuratorFramework connector =  CuratorFrameworkFactory.builder().namespace(nameSpace).connectString(zooKeeperAddress).retryPolicy(new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries)).build();

        System.out.println("Connecting to Zookeeper...");
        try
        {
            //Print the state of zookeeper connection
            System.out.println("The status before call start(): " + connector.getState().name());
            connector.start();
            //block the process until the client connect to Zookeeper
            connector.blockUntilConnected();
            //Print the state of zookeeper connection
            System.out.println("The status after call start(): " + connector.getState().name());
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (connector != null)
            {
                connector.close();
                System.out.println("The Zookeeper connection is closed");
            }
        }
    }
}
