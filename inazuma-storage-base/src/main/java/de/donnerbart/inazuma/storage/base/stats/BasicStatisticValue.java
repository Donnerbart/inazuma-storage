package de.donnerbart.inazuma.storage.base.stats;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

@SuppressWarnings("unused")
public final class BasicStatisticValue extends AbstractStatisticValue<Long>
{
	private final LongAdder timeRangeValue = new LongAdder();

	public static BasicStatisticValue getInstanceOf(final String group, final String name)
	{
		final BasicStatisticValue candidate = (BasicStatisticValue) StatisticManager.getInstance().getExistingStatisticValue(name);
		if (candidate != null)
		{
			return candidate;
		}

		return new BasicStatisticValue(group, name);
	}

	private BasicStatisticValue(final String group, final String name, final long duration, final TimeUnit timeUnit, final boolean autoRegister)
	{
		super(group, name, duration, timeUnit);
		lastTimeRangedValueDefault = 0L;
		stats.add(Stat.SUM);
		if (autoRegister)
		{
			StatisticManager.getInstance().registerStatisticValue(this);
		}
	}

	private BasicStatisticValue(final String group, final String name, final long duration, final TimeUnit timeUnit)
	{
		this(group, name, duration, timeUnit, true);
	}

	private BasicStatisticValue(final String name, final long duration, final TimeUnit timeUnit)
	{
		this(null, name, duration, timeUnit, true);
	}

	private BasicStatisticValue(final String group, final String name)
	{
		this(group, name, DEFAULT_INTERVAL, DEFAULT_UNIT, true);
	}

	private BasicStatisticValue(final String name)
	{
		this(null, name);
	}

	public void increment()
	{
		timeRangeValue.increment();
	}

	public void increment(final long value)
	{
		timeRangeValue.add(value);
	}

	@Override
	protected String getType()
	{
		return "java.lang.Long";
	}

	@Override
	protected void swapValue()
	{
		lastTimeRangedValueDefault = timeRangeValue.sumThenReset();
	}
}
