package de.donnerbart.inazuma.storage.base.stats;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

@SuppressWarnings("unused")
public final class AdvancedStatisticValue extends AbstractStatisticValue<Long>
{
	private final LongAdder timeRangeValue = new LongAdder();
	private final AtomicLong timeRangeValueMin = new AtomicLong(Long.MAX_VALUE);
	private final AtomicLong timeRangeValueMax = new AtomicLong(Long.MIN_VALUE);
	private final LongAdder invocations = new LongAdder();

	public static AdvancedStatisticValue getInstanceOf(final String group, final String name, final EnumSet<Stat> options)
	{
		final AdvancedStatisticValue candidate = (AdvancedStatisticValue) StatisticManager.getInstance().getExistingStatisticValue(name);
		if (candidate != null)
		{
			return candidate;
		}

		return new AdvancedStatisticValue(group, name, options);
	}

	private AdvancedStatisticValue(final String group, final String name, final long duration, final TimeUnit timeUnit, final EnumSet<Stat> options, final boolean autoRegister)
	{
		super(group, name, duration, timeUnit);

		stats.addAll(options);
		if (showSum())
		{
			lastTimeRangedValueDefault = 0L;
		}
		if (showCount())
		{
			lastTimeRangedValueCount = 0L;
		}
		if (showAvg())
		{
			lastTimeRangedValueAvg = 0L;
		}
		if (showMin())
		{
			lastTimeRangedValueMin = 0L;
		}
		if (showMax())
		{
			lastTimeRangedValueMax = 0L;
		}
		if (autoRegister)
		{
			StatisticManager.getInstance().registerStatisticValue(this);
		}
	}

	private AdvancedStatisticValue(final String group, final String name, final long duration, final TimeUnit timeUnit, final EnumSet<Stat> options)
	{
		this(group, name, duration, timeUnit, options, true);
	}

	private AdvancedStatisticValue(final String name, final long duration, final TimeUnit timeUnit, final EnumSet<Stat> options)
	{
		this(null, name, duration, timeUnit, options, true);
	}

	private AdvancedStatisticValue(final String group, final String name, final EnumSet<Stat> options)
	{
		this(group, name, DEFAULT_INTERVAL, DEFAULT_UNIT, options);
	}

	private AdvancedStatisticValue(final String name, final EnumSet<Stat> options)
	{
		this(null, name, options);
	}

	public void increment(final long value)
	{
		timeRangeValue.add(value);
		invocations.increment();
		final long oldMin = timeRangeValueMin.get();
		if (value < oldMin)
		{
			timeRangeValueMin.compareAndSet(oldMin, value);
		}
		final long oldMax = timeRangeValueMax.get();
		if (value > oldMax)
		{
			timeRangeValueMax.compareAndSet(oldMax, value);
		}
	}

	@Override
	protected String getType()
	{
		return "java.lang.Long";
	}

	@Override
	protected void swapValue()
	{
		final long value = timeRangeValue.sumThenReset();
		final long inv = invocations.sumThenReset();
		if (showSum())
		{
			lastTimeRangedValueDefault = value;
		}
		if (showCount())
		{
			lastTimeRangedValueCount = inv;
		}
		if (showAvg())
		{
			lastTimeRangedValueAvg = (inv == 0) ? 0L : Math.round((double) value / inv);
		}
		if (showMin())
		{
			final long showMin = timeRangeValueMin.getAndSet(Long.MAX_VALUE);
			lastTimeRangedValueMin = (showMin == Long.MAX_VALUE) ? 0L : showMin;
		}
		if (showMax())
		{
			final long showMax = timeRangeValueMax.getAndSet(Long.MIN_VALUE);
			lastTimeRangedValueMax = (showMax == Long.MIN_VALUE) ? 0L : showMax;
		}
	}
}
