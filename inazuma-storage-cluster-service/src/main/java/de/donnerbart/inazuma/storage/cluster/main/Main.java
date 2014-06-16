package de.donnerbart.inazuma.storage.cluster.main;

import de.donnerbart.inazuma.storage.cluster.inazuma.InazumaStorageManager;

import java.util.concurrent.CountDownLatch;

public class Main
{
	public static void main(final String[] args)
	{
		final CountDownLatch latch = InazumaStorageManager.start();

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
