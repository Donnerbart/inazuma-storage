package de.donnerbart.inazuma.storage.benchmark.actor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;

public class AddDocumentParameters
{
	private final LongAdder durationAdder;
	private final LongAdder invocationAdder;

	private final CountDownLatch startLatch;
	private final CountDownLatch resultLatch;

	public AddDocumentParameters(final LongAdder durationAdder, final LongAdder invocationAdder, final CountDownLatch startLatch, final CountDownLatch resultLatch)
	{
		this.durationAdder = durationAdder;
		this.invocationAdder = invocationAdder;
		this.startLatch = startLatch;
		this.resultLatch = resultLatch;
	}

	public LongAdder getDurationAdder()
	{
		return durationAdder;
	}

	public LongAdder getInvocationAdder()
	{
		return invocationAdder;
	}

	public CountDownLatch getStartLatch()
	{
		return startLatch;
	}

	public CountDownLatch getResultLatch()
	{
		return resultLatch;
	}
}
