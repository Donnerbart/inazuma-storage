package inazuma;

import com.couchbase.client.CouchbaseClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import jmx.JMXAgent;
import request.RequestController;
import stats.StatisticManager;
import storage.StorageController;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class InazumaStorageManager
{
	private static final CountDownLatch latch = new CountDownLatch(1);
	private static final AtomicReference<StorageController> storageControllerReference = new AtomicReference<>(null);

	public static CountDownLatch start()
	{
		// Get Hazelcast instance
		final HazelcastInstance hz = HazelcastManager.getInstance();

		// Get Couchbase connection
		final CouchbaseClient cb = CouchbaseManager.getClient();

		// Start JMX agent
		new JMXAgent();

		// Startup storage controller
		final StorageController storageController = new StorageController(cb);
		storageControllerReference.set(storageController);

		// Startup request controller
		new RequestController(hz, storageController);

		// Create shutdown event
		Runtime.getRuntime().addShutdownHook(new Thread(new HazelcastShutdownHook()));

		return latch;
	}

	private static class HazelcastShutdownHook implements Runnable
	{
		@Override
		public void run()
		{
			System.out.println("Received shutdown signal!");

			// Shutdown request storage
			System.out.println("Shutting down RequestController...");
			RequestController.getInstance().shutdown();
			System.out.println("Done!\n");

			// Shutdown storage threads
			System.out.println("Shutting down StorageController...");
			final StorageController storageController = storageControllerReference.get();
			if (storageController != null)
			{
				storageController.shutdown();
				storageController.awaitShutdown();
				storageControllerReference.set(null);
			}
			System.out.println("Done!\n");

			// Shutdown of connection inazuma
			System.out.println("Shutting down ConnectionManager...");
			CouchbaseManager.shutdown();
			System.out.println("Done!\n");

			// Shutdown of Hazelcast instance
			System.out.println("Shutting down Hazelcast instance...");
			Hazelcast.shutdownAll();
			System.out.println("Done!\n");

			// Shutdown of StatisticManager
			System.out.println("Shutting down StatisticManager...");
			StatisticManager.getInstance().shutdown();
			System.out.println("Done!\n");

			// Release main thread
			latch.countDown();
		}
	}
}
