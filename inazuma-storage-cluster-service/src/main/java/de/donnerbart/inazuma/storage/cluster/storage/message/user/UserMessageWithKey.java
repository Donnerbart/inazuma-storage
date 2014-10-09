package de.donnerbart.inazuma.storage.cluster.storage.message.user;

abstract class UserMessageWithKey implements UserMessage
{
	private final String userID;
	private final String key;

	public UserMessageWithKey(final String userID, final String key)
	{
		this.userID = userID;
		this.key = key;
	}

	@Override
	public String getUserID()
	{
		return userID;
	}

	public String getKey()
	{
		return key;
	}
}
