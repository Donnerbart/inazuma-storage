package de.donnerbart.inazuma.storage.base.stats;

import java.util.concurrent.TimeUnit;

@SuppressWarnings("unused")
public class CustomStatisticValue<VALUE> extends AbstractStatisticValue<VALUE>
{
	private final ValueCollector<VALUE> valueCollector;

	@SuppressWarnings("unchecked")
	public static <VALUE> CustomStatisticValue<VALUE> getInstanceOf(final String group, final String name, final ValueCollector<VALUE> valueCollector)
	{
		final CustomStatisticValue<VALUE> candidate = (CustomStatisticValue<VALUE>) StatisticManager.getInstance().getExistingStatisticValue(name);
		if (candidate != null)
		{
			return candidate;
		}

		return new CustomStatisticValue<>(group, name, valueCollector);
	}

	private CustomStatisticValue(final String group, final String name, final long duration, final TimeUnit timeUnit, final ValueCollector<VALUE> valueCollector)
	{
		super(group, name, duration, timeUnit);
		this.valueCollector = valueCollector;
		stats.add(Stat.SUM);
		swapValue();

		StatisticManager.getInstance().registerStatisticValue(this);
	}

	private CustomStatisticValue(final String group, final String name, final ValueCollector<VALUE> valueCollector)
	{
		this(group, name, DEFAULT_INTERVAL, DEFAULT_UNIT, valueCollector);
	}

	private CustomStatisticValue(final String name, final long duration, final TimeUnit timeUnit, final ValueCollector<VALUE> valueCollector)
	{
		this(null, name, duration, timeUnit, valueCollector);
	}

	private CustomStatisticValue(final String name, final ValueCollector<VALUE> valueCollector)
	{
		this(null, name, DEFAULT_INTERVAL, DEFAULT_UNIT, valueCollector);
	}

	@Override
	protected String getType()
	{
		return valueCollector.getType();
	}

	@Override
	protected void swapValue()
	{
		lastTimeRangedValueDefault = valueCollector.collectValue();
	}

	@SuppressWarnings("SameReturnValue")
	public static interface ValueCollector<VALUE>
	{
		VALUE collectValue();

		String getType();
	}
}
