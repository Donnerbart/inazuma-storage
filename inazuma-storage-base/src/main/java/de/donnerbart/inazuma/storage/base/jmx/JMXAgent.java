package de.donnerbart.inazuma.storage.base.jmx;

import de.donnerbart.inazuma.storage.base.stats.StatisticManager;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

public class JMXAgent
{
	private static final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

	public JMXAgent(final String domain, final String type)
	{
		try
		{
			mbs.registerMBean(new InazumaStorageRequestWrapper(), new ObjectName(domain + ":type=" + type));

			// Provide StatisticManager with data for JMX agent
			StatisticManager.getInstance().registerMBean(mbs, domain + ":type=StatisticManager");
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}
}
