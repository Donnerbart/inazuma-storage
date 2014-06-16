package de.donnerbart.inazuma.storage.cluster.storage.message;

import de.donnerbart.inazuma.storage.cluster.storage.callback.BlockingCallback;

public class BaseCallbackMessageWithKey<T> extends BaseMessageWithKey
{
	private final BlockingCallback<T> callback;

	public BaseCallbackMessageWithKey(final MessageType type, final String userID, final String key)
	{
		super(type, userID, key);
		this.callback = new BlockingCallback<>();
	}

	public void setResult(final T result)
	{
		callback.setResult(result);
	}

	public BlockingCallback<T> getCallback()
	{
		return callback;
	}
}
