package de.donnerbart.inazuma.storage.cluster.storage.callback;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class BlockingCallback<T>
{
	private final AtomicReference<T> result = new AtomicReference<>(null);
	private final CountDownLatch countDownLatch = new CountDownLatch(1);

	public T getResult()
	{
		try
		{
			countDownLatch.await(5, TimeUnit.MINUTES);
		}
		catch (InterruptedException ignored)
		{
		}
		return result.get();
	}

	public void setResult(final T result)
	{
		this.result.set(result);
		this.countDownLatch.countDown();
	}
}
