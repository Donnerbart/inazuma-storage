package de.donnerbart.inazuma.storage.cluster.storage.message.control;

public class ShutdownMessage implements ControlMessage
{
	private static final ShutdownMessage INSTANCE = new ShutdownMessage();

	public static ShutdownMessage getInstance()
	{
		return INSTANCE;
	}
}
