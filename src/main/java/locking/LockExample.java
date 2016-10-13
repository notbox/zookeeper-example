package locking;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LockExample implements Runnable
{
    private final InterProcessMutex lock;
    private final String clientName;
    private final CuratorFramework zooClient;
    private final int waitLockSecond = 3;
    private int lockCount = 0;
    private final int keepSeconds = 3;
    private final int maxLockCount = 3;

    public LockExample(String zooKeeperAddress, int baseSleepTimeMs, int maxRetries, String path, String clientName)
    {
        this.clientName = clientName;
        this.zooClient = CuratorFrameworkFactory.newClient(zooKeeperAddress, new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries));

        lock = new InterProcessMutex(zooClient, path);
        zooClient.start();
    }

    public void process() throws Exception
    {
        System.out.println(this.clientName + " is attempting to get the lock...");

        if ( lock.acquire(this.waitLockSecond, TimeUnit.SECONDS) )
        {
            try
            {
                this.lockCount ++;
                System.out.println(clientName + " will keep the lock for " + this.keepSeconds + " seconds... (" + this.lockCount + ")");
                Thread.sleep(this.keepSeconds*1000);
            }
            finally
            {
                System.out.println(clientName + " releasing the lock");
                // always release the lock in a finally block
                lock.release();
                System.out.println("-----------------------");
            }
        }
    }

    public void close()
    {
        zooClient.close();
    }

    public void run()
    {
        try
        {
            while (this.lockCount < this.maxLockCount)
            {
                Thread.sleep(1*1000);
                process();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static void main (String[] args)
    {
        String zooKeeperAddress = "192.168.56.101:2181,192.168.56.101:2182,192.168.56.101:2183";
        int baseSleepTimeMs = 1000;
        int maxRetries = 3;
        int clientNum = 10;
        String zookeeperPath = "/School/Lock";

        ExecutorService executor = Executors.newFixedThreadPool(clientNum);
        List<LockExample> lockExamples = new ArrayList<LockExample>();
        try
        {
            for (int i = 0; i < clientNum; i ++)
            {
                LockExample lockExample = new LockExample(zooKeeperAddress, baseSleepTimeMs, maxRetries, zookeeperPath, "Client[" + i + "]");
                executor.execute(lockExample);
                lockExamples.add(lockExample);
            }
            executor.shutdown();

            //Wait for all the threads are done
            while (!executor.isTerminated())
            {
                //Sleep for 1 second
                Thread.sleep(1000);
            }
            System.out.println("All clients are done!");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            for(LockExample lockExample: lockExamples)
            {
                lockExample.close();
            }
            System.out.println("The Zookeeper clients are closed!");
        }
    }
}
