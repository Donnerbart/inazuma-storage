package de.donnerbart.inazuma.storage.cluster;

import java.util.concurrent.CountDownLatch;

public class Main
{
	public static void main(final String[] args)
	{
		final CountDownLatch latch = InazumaStorageClusterService.start("default");

		// Wait for shutdown hook
		System.out.println("Inazuma-Storage is running...");
		try
		{
			latch.await();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}

		System.out.println("Inazuma-Storage is shut down!");
		System.exit(0);
	}
}
