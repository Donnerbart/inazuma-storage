package de.donnerbart.inazuma.storage.cluster;

import com.couchbase.client.CouchbaseClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import de.donnerbart.inazuma.storage.base.manager.CouchbaseManager;
import de.donnerbart.inazuma.storage.base.manager.HazelcastManager;
import de.donnerbart.inazuma.storage.base.jmx.JMXAgent;
import de.donnerbart.inazuma.storage.base.request.RequestController;
import de.donnerbart.inazuma.storage.base.stats.StatisticManager;
import de.donnerbart.inazuma.storage.cluster.storage.StorageController;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class InazumaStorageClusterService
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
		new JMXAgent("de.donnerbart", "inazuma.storage.cluster");

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

			// Shutdown RequestController
			System.out.println("Shutting down RequestController...");
			RequestController.getInstance().shutdown();
			System.out.println("Done!\n");

			// Shutdown StorageController
			System.out.println("Shutting down StorageController...");
			final StorageController storageController = storageControllerReference.get();
			if (storageController != null)
			{
				storageController.shutdown();
				storageController.awaitShutdown();
				storageControllerReference.set(null);
			}
			System.out.println("Done!\n");

			// Shutdown of Couchbase instance
			System.out.println("Shutting down Couchbase instance...");
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
