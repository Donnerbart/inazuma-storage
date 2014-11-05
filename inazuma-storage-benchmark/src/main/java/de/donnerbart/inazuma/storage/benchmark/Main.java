package de.donnerbart.inazuma.storage.benchmark;

import de.donnerbart.inazuma.storage.base.request.AddPersistenceLevel;
import de.donnerbart.inazuma.storage.benchmark.jmx.InazumaStorageBenchmarkWrapper;

public class Main
{
	private static final boolean AUTO_START;
	private static final int NUMBER_OF_DOCUMENTS;
	private static final int NUMBER_OF_ACTORS;
	private static final AddPersistenceLevel PERSISTENCE_LEVEL;

	static
	{
		AUTO_START = Boolean.valueOf(System.getProperty("autostart", "false"));
		NUMBER_OF_DOCUMENTS = Integer.valueOf(System.getProperty("documents", "10000"));
		NUMBER_OF_ACTORS = Integer.valueOf(System.getProperty("actors", "100"));

		switch (System.getProperty("persistence", "default"))
		{
			case "queued":
			{
				PERSISTENCE_LEVEL = AddPersistenceLevel.REQUEST_ON_QUEUE;
				break;
			}
			case "document_persisted":
			{
				PERSISTENCE_LEVEL = AddPersistenceLevel.DOCUMENT_PERSISTED;
				break;
			}
			case "document_metadata_added":
			{
				PERSISTENCE_LEVEL = AddPersistenceLevel.DOCUMENT_METADATA_ADDED;
				break;
			}
			default:
			{
				PERSISTENCE_LEVEL = AddPersistenceLevel.DEFAULT_LEVEL;
			}
		}
	}

	public static void main(final String[] args)
	{
		final InazumaStorageBenchmark inazumaStorage = new InazumaStorageBenchmark();
		System.out.println("Inazuma-Storage-Benchmark is running...");

		if (AUTO_START)
		{
			if (NUMBER_OF_ACTORS <= 0 || NUMBER_OF_DOCUMENTS <= 0)
			{
				System.out.println("Negative numbers are not allowed as number of actors or documents!");
				inazumaStorage.shutdown();

				awaitShutdownAndExit(inazumaStorage, 1);
			}
			if (NUMBER_OF_ACTORS > NUMBER_OF_DOCUMENTS)
			{
				System.out.println("We can't have more actors than documents!");
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
			inazumaStorageBenchmarkWrapper.setDefaultNumberOfActors(NUMBER_OF_ACTORS);
			final long duration = inazumaStorageBenchmarkWrapper.insertMultipleDocuments(NUMBER_OF_DOCUMENTS, NUMBER_OF_ACTORS, PERSISTENCE_LEVEL, true);

			inazumaStorage.shutdown();

			System.out.println("Stats: " + inazumaStorageBenchmarkWrapper.getStatistics() + " with "
							+ NUMBER_OF_ACTORS + " actors and persistence level "
							+ PERSISTENCE_LEVEL + " finished in "
							+ duration + " seconds\n"
			);
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
