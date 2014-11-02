package de.donnerbart.inazuma.storage.benchmark;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import de.donnerbart.inazuma.storage.base.jmx.JMXAgent;
import de.donnerbart.inazuma.storage.base.manager.HazelcastManager;
import de.donnerbart.inazuma.storage.base.request.RequestController;
import de.donnerbart.inazuma.storage.base.stats.StatisticManager;
import de.donnerbart.inazuma.storage.benchmark.jmx.InazumaStorageBenchmarkWrapper;

import java.util.concurrent.CountDownLatch;

public class InazumaStorageBenchmark
{
	private final CountDownLatch latch;

	public InazumaStorageBenchmark()
	{
		// Get Hazelcast instance
		final HazelcastInstance hz = HazelcastManager.getClientInstance();

		// Start JMX agent
		new JMXAgent("benchmark", new InazumaStorageBenchmarkWrapper());

		// Startup request wrapper
		new RequestController(hz, null);

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
}
