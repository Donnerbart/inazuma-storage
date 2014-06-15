package storage.message;

import storage.callback.BlockingCallback;

public class BaseCallbackMessage<T> extends BaseMessage
{
	private final BlockingCallback<T> callback;

	public BaseCallbackMessage(final MessageType type, final String userID)
	{
		super(type, userID);
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
