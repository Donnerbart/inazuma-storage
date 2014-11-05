package de.donnerbart.inazuma.storage.benchmark.jmx;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import de.donnerbart.inazuma.storage.base.jmx.InazumaStorageJMXBean;
import de.donnerbart.inazuma.storage.benchmark.actor.ActorFactory;
import de.donnerbart.inazuma.storage.benchmark.actor.AddDocumentParameters;
import scala.concurrent.duration.Duration;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class InazumaStorageBenchmarkWrapper implements InazumaStorageBenchmarkWrapperMBean, InazumaStorageJMXBean
{
	public final static int MAX_NUMBER_OF_ACTORS = 10000;
	public final static int MAX_NUMBER_OF_DOCUMENTS = 5000000;

	private final ExecutorService service;

	private final LongAdder durationAdder;
	private final LongAdder invocationAdder;

	private int numberOfActors = 10;

	public InazumaStorageBenchmarkWrapper()
	{
		service = Executors.newSingleThreadExecutor();

		durationAdder = new LongAdder();
		invocationAdder = new LongAdder();
	}

	@Override
	public String getStatistics()
	{
		final long invocations = invocationAdder.sum();
		if (invocations == 0)
		{
			return "no stats";
		}

		final long duration = durationAdder.sum();
		return (duration / invocations) + " ms avg after " + invocations + " invocations";
	}

	@Override
	public void resetStatistics()
	{
		durationAdder.reset();
		invocationAdder.reset();
	}

	@Override
	public int getNumberOfActors()
	{
		return numberOfActors;
	}

	@Override
	public void setNumberOfActors(final int numberOfActors)
	{
		if (numberOfActors > 0 && numberOfActors <= MAX_NUMBER_OF_ACTORS)
		{
			this.numberOfActors = numberOfActors;
		}
	}

	@Override
	public void insertDocuments1()
	{
		insertMultipleDocuments(1);
	}

	@Override
	public void insertDocuments1k()
	{
		insertMultipleDocuments(1000);
	}

	@Override
	public void insertDocuments10k()
	{
		insertMultipleDocuments(10000);
	}

	@Override
	public void insertDocuments50k()
	{
		insertMultipleDocuments(50000);
	}

	@Override
	public void insertDocuments100k()
	{
		insertMultipleDocuments(100000);
	}

	@Override
	public void insertMultipleDocuments(final int numberOfDocuments)
	{
		insertMultipleDocuments(numberOfDocuments, false);
	}

	public long insertMultipleDocuments(final int numberOfDocuments, final boolean await)
	{
		if (numberOfDocuments > MAX_NUMBER_OF_DOCUMENTS)
		{
			return -1;
		}

		final AtomicLong started = new AtomicLong(0);
		final CountDownLatch allDoneLatch = new CountDownLatch(1);

		// Start insert task on single thread executor to return JMX call fast
		service.submit(() -> {
			final CountDownLatch startLatch = new CountDownLatch(1);
			final CountDownLatch resultLatch = new CountDownLatch(numberOfDocuments);
			final AddDocumentParameters parameters = new AddDocumentParameters(durationAdder, invocationAdder, startLatch, resultLatch);

			final ActorSystem actorSystem = ActorSystem.create();
			final ActorRef[] actors = new ActorRef[numberOfActors];

			System.out.println("Creating " + numberOfActors + " concurrent actors...");
			for (int i = 0; i < numberOfActors; i++)
			{
				actors[i] = ActorFactory.createMessageDispatcher(actorSystem, parameters);
			}

			System.out.println("Queueing " + numberOfDocuments + " documents...");
			for (int i = 0; i < numberOfDocuments; i++)
			{
				actors[i % numberOfActors].tell("", ActorRef.noSender());
			}

			System.out.println("Inserting documents...");
			started.set(System.nanoTime());
			startLatch.countDown();

			try
			{
				System.out.println("Waiting for actors to complete...");
				resultLatch.await();
				System.out.println("Shutdown actor system...");
				actorSystem.shutdown();
				System.out.println("Awaiting termination...");
				actorSystem.awaitTermination(Duration.create(10, TimeUnit.SECONDS));

				allDoneLatch.countDown();
			}
			catch (InterruptedException ignored)
			{
			}
			System.out.println("Done...");
		});

		if (await)
		{
			try
			{
				while (allDoneLatch.getCount() > 0)
				{
					System.out.println("Stats: " + getStatistics() + " in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started.get()) + " seconds");

					allDoneLatch.await(10, TimeUnit.SECONDS);
				}
			}
			catch (InterruptedException ignored)
			{
			}
		}

		return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started.get());
	}
}
