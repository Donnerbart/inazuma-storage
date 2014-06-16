package de.donnerbart.inazuma.storage.cluster.storage.message;

public class BaseMessageWithKey extends BaseMessage
{
	private final String key;

	public BaseMessageWithKey(final MessageType type, final String userID, final String key)
	{
		super(type, userID);
		this.key = key;
	}

	public String getKey()
	{
		return key;
	}
}
