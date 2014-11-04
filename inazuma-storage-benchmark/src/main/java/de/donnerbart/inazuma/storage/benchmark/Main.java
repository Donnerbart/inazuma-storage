package de.donnerbart.inazuma.storage.benchmark;

import de.donnerbart.inazuma.storage.benchmark.jmx.InazumaStorageBenchmarkWrapper;

public class Main
{
	private static final boolean AUTOSTART;
	private static final int NUMBER_OF_DOCUMENTS;
	private static final int NUMBER_OF_THREADS;

	static
	{
		AUTOSTART = Boolean.valueOf(System.getProperty("autostart", "false"));
		NUMBER_OF_DOCUMENTS = Integer.valueOf(System.getProperty("documents", "1000"));
		NUMBER_OF_THREADS = Integer.valueOf(System.getProperty("threads", "10"));
	}

	public static void main(final String[] args)
	{
		final InazumaStorageBenchmark inazumaStorage = new InazumaStorageBenchmark();

		if (AUTOSTART)
		{
			if (NUMBER_OF_THREADS <= 0 || NUMBER_OF_DOCUMENTS <= 0)
			{
				System.out.println("Negative numbers are not allowed here!");
				inazumaStorage.shutdown();

				awaitShutdownAndExit(inazumaStorage, 1);
			}

			final InazumaStorageBenchmarkWrapper inazumaStorageBenchmarkWrapper = new InazumaStorageBenchmarkWrapper();
			inazumaStorageBenchmarkWrapper.setThreadPoolSize(NUMBER_OF_THREADS);
			inazumaStorageBenchmarkWrapper.insertMultipleDocuments(NUMBER_OF_DOCUMENTS, true);

			inazumaStorage.shutdown();
		}

		awaitShutdownAndExit(inazumaStorage, 0);
	}

	private static void awaitShutdownAndExit(final InazumaStorageBenchmark inazumaStorage, final int exitCode)
	{
		// Wait for shutdown hook
		System.out.println("Inazuma-Storage-Benchmark is running...");
		inazumaStorage.await();

		System.out.println("Inazuma-Storage-Benchmark is shut down!");
		System.exit(exitCode);
	}
}
