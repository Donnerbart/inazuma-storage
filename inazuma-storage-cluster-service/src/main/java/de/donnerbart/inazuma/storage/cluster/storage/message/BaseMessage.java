package de.donnerbart.inazuma.storage.cluster.storage.message;

public class BaseMessage
{
	private final MessageType type;
	private final String userID;

	public BaseMessage(final MessageType type, final String userID)
	{
		this.type = type;
		this.userID = userID;
	}

	public MessageType getType()
	{
		return type;
	}

	public String getUserID()
	{
		return userID;
	}
}
