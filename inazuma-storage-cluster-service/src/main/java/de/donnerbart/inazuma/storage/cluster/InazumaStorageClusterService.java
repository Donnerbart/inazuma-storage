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
import java.util.concurrent.atomic.AtomicInteger;

public class InazumaStorageClusterService
{
	private static final AtomicInteger instanceCounter = new AtomicInteger(0);

	private final String instanceNumberString;

	private final StorageControllerFacade storageController;
	private final RequestController requestController;
	private final CountDownLatch latch;

	public InazumaStorageClusterService(final String bucketName, final boolean createWithInstanceNumber)
	{
		final int instanceNumber;
		if (createWithInstanceNumber)
		{
			instanceNumber = instanceCounter.incrementAndGet();
			instanceNumberString = " #" + instanceNumber;
		}
		else
		{
			instanceNumber = 0;
			instanceNumberString = "";
		}

		// Get Hazelcast instance
		final HazelcastInstance hz = HazelcastManager.getInstance();

		// Get Couchbase instance
		final AsyncBucket bucket = CouchbaseManager.getAsyncBucket(bucketName);
		final DatabaseWrapper databaseWrapper = new CouchbaseWrapper(bucket);

		// Start JMX agent
		new JMXAgent("de.donnerbart", "inazuma.storage.cluster-" + instanceNumber);

		// Start StorageController
		storageController = new StorageController(databaseWrapper, instanceNumber);

		// Start RequestController
		requestController = new RequestController(hz, storageController);

		// Create shutdown event
		Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));

		latch = new CountDownLatch(1);
	}

	public void shutdown()
	{
		if (latch.getCount() == 0)
		{
			return;
		}

		System.out.println("Shutdown of Inazuma-Storage" + instanceNumberString + " requested...\n");

		// Shutdown RequestController
		System.out.println("Shutting down RequestController" + instanceNumberString + "...");
		requestController.shutdown();
		System.out.println("Done!\n");

		// Shutdown StorageController
		System.out.println("Shutting down StorageController" + instanceNumberString + "...");
		storageController.shutdown();
		System.out.println("Done!\n");

		// Shutdown of Couchbase instance
		System.out.println("Shutting down Couchbase instance" + instanceNumberString + "...");
		CouchbaseManager.shutdown();
		System.out.println("Done!\n");

		// Shutdown of Hazelcast instance
		System.out.println("Shutting down Hazelcast instance" + instanceNumberString + "...");
		Hazelcast.shutdownAll();
		System.out.println("Done!\n");

		// Shutdown of StatisticManager
		System.out.println("Shutting down StatisticManager...");
		StatisticManager.getInstance().shutdown();
		System.out.println("Done!\n");

		// Release main thread
		latch.countDown();
	}

	public void await()
	{
		try
		{
			latch.await();
		}
		catch (InterruptedException ignored)
		{
		}
	}

	public RequestController getRequestController()
	{
		return requestController;
	}
}
