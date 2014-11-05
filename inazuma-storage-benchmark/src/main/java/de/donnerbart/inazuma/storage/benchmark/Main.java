package de.donnerbart.inazuma.storage.benchmark;

import de.donnerbart.inazuma.storage.benchmark.jmx.InazumaStorageBenchmarkWrapper;

public class Main
{
	private static final boolean AUTO_START;
	private static final int NUMBER_OF_DOCUMENTS;
	private static final int NUMBER_OF_ACTORS;

	static
	{
		AUTO_START = Boolean.valueOf(System.getProperty("autostart", "false"));
		NUMBER_OF_DOCUMENTS = Integer.valueOf(System.getProperty("documents", "1000"));
		NUMBER_OF_ACTORS = Integer.valueOf(System.getProperty("actors", "10"));
	}

	public static void main(final String[] args)
	{
		final InazumaStorageBenchmark inazumaStorage = new InazumaStorageBenchmark();
		System.out.println("Inazuma-Storage-Benchmark is running...");

		if (AUTO_START)
		{
			if (NUMBER_OF_ACTORS <= 0 || NUMBER_OF_DOCUMENTS <= 0)
			{
				System.out.println("Negative numbers are not allowed here!");
				inazumaStorage.shutdown();

				awaitShutdownAndExit(inazumaStorage, 1);
			}
			if (NUMBER_OF_ACTORS > NUMBER_OF_DOCUMENTS)
			{
				System.out.println("We don't need more actors than documents!");
				inazumaStorage.shutdown();

				awaitShutdownAndExit(inazumaStorage, 1);
			}
			if (NUMBER_OF_ACTORS > InazumaStorageBenchmarkWrapper.MAX_NUMBER_OF_ACTORS || NUMBER_OF_DOCUMENTS > InazumaStorageBenchmarkWrapper.MAX_NUMBER_OF_DOCUMENTS)
			{
				System.out.println("That's a bit too much!");
				inazumaStorage.shutdown();

				awaitShutdownAndExit(inazumaStorage, 1);
			}

			final InazumaStorageBenchmarkWrapper inazumaStorageBenchmarkWrapper = new InazumaStorageBenchmarkWrapper();
			inazumaStorageBenchmarkWrapper.setNumberOfActors(NUMBER_OF_ACTORS);
			final long duration = inazumaStorageBenchmarkWrapper.insertMultipleDocuments(NUMBER_OF_DOCUMENTS, true);

			inazumaStorage.shutdown();

			System.out.println("Stats: " + inazumaStorageBenchmarkWrapper.getStatistics() + " with " + NUMBER_OF_ACTORS + " actors in " + duration + " seconds\n");
		}

		awaitShutdownAndExit(inazumaStorage, 0);
	}

	private static void awaitShutdownAndExit(final InazumaStorageBenchmark inazumaStorage, final int exitCode)
	{
		// Wait for shutdown hook
		inazumaStorage.await();

		System.out.println("Inazuma-Storage-Benchmark is shut down!");
		System.exit(exitCode);
	}
}
