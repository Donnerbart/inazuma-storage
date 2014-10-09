package de.donnerbart.inazuma.storage.cluster.storage.message.control;

public class RemoveIdleMessageProcessorMessage implements ControlMessage
{
	private final String userID;

	public RemoveIdleMessageProcessorMessage(final String userID)
	{
		this.userID = userID;
	}

	public String getUserID()
	{
		return userID;
	}
}
