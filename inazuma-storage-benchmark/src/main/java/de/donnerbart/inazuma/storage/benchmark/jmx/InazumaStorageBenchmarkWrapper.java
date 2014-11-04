package de.donnerbart.inazuma.storage.benchmark.jmx;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import de.donnerbart.inazuma.storage.base.jmx.InazumaStorageJMXBean;
import de.donnerbart.inazuma.storage.base.request.AddPersistenceLevel;
import de.donnerbart.inazuma.storage.base.request.RequestController;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class InazumaStorageBenchmarkWrapper implements InazumaStorageBenchmarkWrapperMBean, InazumaStorageJMXBean
{
	private static final int MAX_USER = 100000;
	private static final IntObjectOpenHashMap<String> MAILS = new IntObjectOpenHashMap<>();
	private static final Random RANDOM = new Random();

	static
	{
		for (int userID = 1; userID <= MAX_USER; userID++)
		{
			MAILS.put(userID, "{\"userID\":" + userID + "}");
		}
	}

	private final ExecutorService service;

	private final LongAdder durationAdder;
	private final LongAdder invocationAdder;

	private int threadPoolSize = 10;

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
		return (duration / invocations) + " ms avg after " + invocations + " runs";
	}

	@Override
	public void resetStatistics()
	{
		durationAdder.reset();
		invocationAdder.reset();
	}

	@Override
	public int getThreadPoolSize()
	{
		return threadPoolSize;
	}

	@Override
	public void setThreadPoolSize(final int threadPoolSize)
	{
		if (threadPoolSize > 0 && threadPoolSize <= 1000)
		{
			this.threadPoolSize = threadPoolSize;
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
	public void insertMultipleDocuments(final int count)
	{
		insertMultipleDocuments(count, false);
	}

	public void insertMultipleDocuments(final int count, final boolean await)
	{
		final CountDownLatch countdown = new CountDownLatch(count);
		final Runnable task = () -> {
			final int userID = RANDOM.nextInt(MAX_USER) + 1;
			final String key = UUID.randomUUID().toString();
			final long created = (System.currentTimeMillis() / 1000) - RANDOM.nextInt(86400);

			final long started = System.nanoTime();
			RequestController.getInstance().addDocument(String.valueOf(userID), key, MAILS.get(userID), created, AddPersistenceLevel.DOCUMENT_METADATA_ADDED);

			durationAdder.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
			invocationAdder.increment();

			countdown.countDown();
		};

		// Start insert task on single thread executor to return JMX call fast
		final CountDownLatch latch = new CountDownLatch(1);
		service.submit(() -> {
			System.out.println("Inserting " + count + " documents with " + threadPoolSize + " concurrent threads...");
			final ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
			for (int i = 0; i < count; i++)
			{
				executorService.submit(task);
			}

			try
			{
				System.out.println("Waiting for threads to complete...");
				countdown.await();
				System.out.println("Request shutdown...");
				executorService.shutdown();
				System.out.println("Awaiting termination...");
				executorService.awaitTermination(10, TimeUnit.SECONDS);
				latch.countDown();
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
				while (latch.getCount() > 0)
				{
					System.out.println("Stats: " + getStatistics());

					latch.await(1, TimeUnit.SECONDS);
				}
				System.out.println("Stats: " + getStatistics());
			}
			catch (InterruptedException ignored)
			{
			}
		}
	}
}
