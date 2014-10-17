package de.donnerbart.inazuma.storage.base.stats;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

@SuppressWarnings("unused")
public final class BasicStatisticValueAvg extends AbstractStatisticValue<Long>
{
	private final LongAdder timeRangeValue = new LongAdder();
	private final LongAdder invocations = new LongAdder();

	public static BasicStatisticValueAvg getInstanceOf(final String group, final String name)
	{
		final BasicStatisticValueAvg candidate = (BasicStatisticValueAvg) StatisticManager.getInstance().getExistingStatisticValue(name);
		if (candidate != null)
		{
			return candidate;
		}

		return new BasicStatisticValueAvg(group, name);
	}

	private BasicStatisticValueAvg(final String group, final String name, final long duration, final TimeUnit timeUnit, final boolean autoRegister)
	{
		super(group, name, duration, timeUnit);
		lastTimeRangedValueAvg = 0L;
		stats.add(Stat.AVG);
		if (autoRegister)
		{
			StatisticManager.getInstance().registerStatisticValue(this);
		}
	}

	private BasicStatisticValueAvg(final String group, final String name, final long duration, final TimeUnit timeUnit)
	{
		this(group, name, duration, timeUnit, true);
	}

	private BasicStatisticValueAvg(final String name, final long duration, final TimeUnit timeUnit)
	{
		this(null, name, duration, timeUnit, true);
	}

	private BasicStatisticValueAvg(final String group, final String name)
	{
		this(group, name, DEFAULT_INTERVAL, DEFAULT_UNIT, true);
	}

	private BasicStatisticValueAvg(final String name)
	{
		this(null, name);
	}

	public void increment(final long value)
	{
		timeRangeValue.add(value);
		invocations.increment();
	}

	@Override
	protected String getType()
	{
		return "java.lang.Long";
	}

	@Override
	protected void swapValue()
	{
		final double value = timeRangeValue.sumThenReset();
		final long inv = invocations.sumThenReset();
		lastTimeRangedValueAvg = (inv == 0) ? 0L : Math.round(value / inv);
	}
}
