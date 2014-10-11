package de.donnerbart.inazuma.storage.cluster;

import com.couchbase.client.java.AsyncBucket;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import de.donnerbart.inazuma.storage.base.jmx.JMXAgent;
import de.donnerbart.inazuma.storage.base.manager.HazelcastManager;
import de.donnerbart.inazuma.storage.base.request.RequestController;
import de.donnerbart.inazuma.storage.base.request.StorageControllerFacade;
import de.donnerbart.inazuma.storage.base.stats.StatisticManager;
import de.donnerbart.inazuma.storage.cluster.storage.StorageController;
import de.donnerbart.inazuma.storage.cluster.storage.manager.CouchbaseManager;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.CouchbaseWrapper;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.DatabaseWrapper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class InazumaStorageClusterService
{
	private static final CountDownLatch latch = new CountDownLatch(1);
	private static final AtomicReference<StorageControllerFacade> storageControllerReference = new AtomicReference<>(null);

	public static CountDownLatch start(final String bucketName)
	{
		// Get Hazelcast instance
		final HazelcastInstance hz = HazelcastManager.getInstance();

		// Get Couchbase instance
		final AsyncBucket bucket = CouchbaseManager.getAsyncBucket(bucketName);
		final DatabaseWrapper databaseWrapper = new CouchbaseWrapper(bucket);

		// Start JMX agent
		new JMXAgent("de.donnerbart", "inazuma.storage.cluster");

		// Start StorageController
		final StorageControllerFacade storageController = new StorageController(databaseWrapper);
		storageControllerReference.set(storageController);

		// Start RequestController
		new RequestController(hz, storageController);

		// Create shutdown event
		Runtime.getRuntime().addShutdownHook(new Thread(new HazelcastShutdownHook()));

		return latch;
	}

	public static void stopBlocking()
	{
		stop();

		try
		{
			latch.await();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}

	public static void stop()
	{
		System.out.println("Received stop signal!");

		shutdown();
	}

	private static class HazelcastShutdownHook implements Runnable
	{
		@Override
		public void run()
		{
			System.out.println("Received shutdown signal!");

			shutdown();
		}
	}

	private static void shutdown()
	{
		if (latch.getCount() == 0)
		{
			return;
		}

		// Shutdown RequestController
		System.out.println("Shutting down RequestController...");
		RequestController.getInstance().shutdown();
		System.out.println("Done!\n");

		// Shutdown StorageController
		System.out.println("Shutting down StorageController...");
		final StorageControllerFacade storageController = storageControllerReference.get();
		if (storageController != null)
		{
			storageController.shutdown();
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
