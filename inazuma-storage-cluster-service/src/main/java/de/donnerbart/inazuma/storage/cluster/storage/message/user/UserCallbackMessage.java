package de.donnerbart.inazuma.storage.cluster.storage.message.user;

import de.donnerbart.inazuma.storage.cluster.storage.callback.BlockingCallback;

abstract class UserCallbackMessage<T> implements UserMessage
{
	private final String userID;
	private final BlockingCallback<T> callback;

	public UserCallbackMessage(final String userID)
	{
		this.userID = userID;
		this.callback = new BlockingCallback<>();
	}

	@Override
	public String getUserID()
	{
		return userID;
	}

	public BlockingCallback<T> getCallback()
	{
		return callback;
	}

	public void setResult(final T result)
	{
		callback.setResult(result);
	}
}
