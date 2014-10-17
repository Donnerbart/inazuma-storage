package de.donnerbart.inazuma.storage.cluster;

public class Main
{
	public static void main(final String[] args)
	{
		final InazumaStorageClusterService inazumaStorage = new InazumaStorageClusterService("default", false);

		// Wait for shutdown hook
		System.out.println("Inazuma-Storage is running...");
		inazumaStorage.await();

		System.out.println("Inazuma-Storage is shut down!");
		System.exit(0);
	}
}
