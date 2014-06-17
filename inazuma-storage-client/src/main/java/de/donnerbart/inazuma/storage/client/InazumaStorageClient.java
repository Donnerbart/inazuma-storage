package de.donnerbart.inazuma.storage.client;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import de.donnerbart.inazuma.storage.base.jmx.JMXAgent;
import de.donnerbart.inazuma.storage.base.manager.HazelcastManager;
import de.donnerbart.inazuma.storage.base.request.RequestController;
import de.donnerbart.inazuma.storage.base.stats.StatisticManager;

import java.util.concurrent.CountDownLatch;

public class InazumaStorageClient
{
	private static final CountDownLatch latch = new CountDownLatch(1);

	public static CountDownLatch start()
	{
		// Get Hazelcast instance
		final HazelcastInstance hz = HazelcastManager.getClientInstance();

		// Start JMX agent
		new JMXAgent("de.donnerbart", "inazuma.storage.client");

		// Startup request wrapper
		new RequestController(hz, null);

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
