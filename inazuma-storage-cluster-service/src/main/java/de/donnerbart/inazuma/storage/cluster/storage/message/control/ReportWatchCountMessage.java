package de.donnerbart.inazuma.storage.cluster.storage.message.control;

public class ReportWatchCountMessage implements ControlMessage
{
	private static final ReportWatchCountMessage INSTANCE = new ReportWatchCountMessage();

	public static ReportWatchCountMessage getInstance()
	{
		return INSTANCE;
	}
}
