package de.donnerbart.inazuma.storage.base.jmx;

import de.donnerbart.inazuma.storage.base.stats.StatisticManager;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

public class JMXAgent
{
	private static final String DOMAIN = "de.donnerbart";
	private static final MBeanServer MBS = ManagementFactory.getPlatformMBeanServer();

	static
	{
		// Provide StatisticManager with data for JMX agent
		StatisticManager.getInstance().registerMBean(MBS, DOMAIN + ":type=StatisticManager");
	}

	public JMXAgent(final String subType)
	{
		this(subType, new InazumaStorageRequestWrapper());
	}

	public JMXAgent(final String subType, final InazumaStorageJMXBean inazumaStorageJMXBean)
	{
		try
		{
			MBS.registerMBean(inazumaStorageJMXBean, new ObjectName(DOMAIN + ":type=inazuma.storage." + subType));
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}
}
