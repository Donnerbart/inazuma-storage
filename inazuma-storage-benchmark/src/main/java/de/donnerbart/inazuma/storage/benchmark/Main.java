package de.donnerbart.inazuma.storage.benchmark;

public class Main
{
	public static void main(final String[] args)
	{
		final InazumaStorageBenchmark inazumaStorage = new InazumaStorageBenchmark();

		// Wait for shutdown hook
		System.out.println("Inazuma-Storage-Benchmark is running...");
		inazumaStorage.await();

		System.out.println("Inazuma-Storage-Benchmark is shut down!");
		System.exit(0);
	}
}
