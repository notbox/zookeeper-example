package operation;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.io.BufferedReader;
import java.io.InputStreamReader;


public class Operator
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

            //check the path first
            if(connector.checkExists().inBackground().forPath(testPath) != null)
            {
                System.out.println("The path " + testPath + " doesn't exist");
            }
            else
            {
                System.out.println("The path " + testPath + " does exist");
                //if the path exists, delete it
                connector.delete().forPath(testPath);
            }

            //create the path
            connector.create().forPath(testPath, testPath.getBytes());
            //get the data of the path
            System.out.println("The data in the path : " + new String(connector.getData().watched().forPath(testPath)));

            //Create a EPHEMERAL node
            connector.create().withMode(CreateMode.EPHEMERAL).forPath("/ephemeral", "ephemeral".getBytes());

            //Check the Ephemeral node in Zookeeper
            System.out.println("Please check the Ephemeral node in Zookeeper, then press enter/return to continue\n");
            new BufferedReader(new InputStreamReader(System.in)).readLine();


            //Clean up the test1 and test2 znodes for transaction
            if (connector.checkExists().inBackground().forPath("/test1") == null)
            {
                connector.delete().forPath("/test1");
            }
            if (connector.checkExists().inBackground().forPath("/test2") == null)
            {
                connector.delete().forPath("/test2");
            }

            //Demo of Zookeeper operation transactions
            System.out.println("Transaction...");

            CuratorTransaction transaction = connector.inTransaction();
            transaction.create().withMode(CreateMode.PERSISTENT).forPath("/test1").and();
            transaction.create().withMode(CreateMode.PERSISTENT).forPath("/test2").and();
            transaction.setData().forPath("/test1", "test1".getBytes()).and();
            transaction.setData().forPath("/test2", "test2".getBytes()).and();
            //Please check the transaction
            System.out.println("Please check the transaction in Zookeeper before we commit it, then press enter/return to continue\n");
            new BufferedReader(new InputStreamReader(System.in)).readLine();

            //Commit the transaction
            ((CuratorTransactionFinal)transaction).commit();

            //Please check the transaction
            System.out.println("Please check the transaction in Zookeeper after we commit it, then press enter/return to continue\n");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
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
