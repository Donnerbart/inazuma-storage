package jmx;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import stats.StatisticManager;

public class JMXAgent
{
    private static final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

    public JMXAgent()
    {
        try
        {
            // Provide StatisticManager with data for JMX agent
            StatisticManager.getInstance().registerMBean(mbs, "de.donnerbart:type=StatisticManager");
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
