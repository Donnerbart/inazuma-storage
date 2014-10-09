package de.donnerbart.inazuma.storage.cluster.storage.message.control;

public class WatchMeMessage implements ControlMessage
{
	private static final WatchMeMessage INSTANCE = new WatchMeMessage();

	public static WatchMeMessage getInstance()
	{
		return INSTANCE;
	}
}
