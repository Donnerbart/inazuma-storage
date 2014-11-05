package de.donnerbart.inazuma.storage.cluster.storage.message.user;

import com.hazelcast.core.AsyncCallableCallback;
import de.donnerbart.inazuma.storage.base.request.AddPersistenceLevel;

public class AddDocumentMessage extends UserMessageWithKey
{
	private final String json;
	private final long created;
	private final AddPersistenceLevel persistenceLevel;
	private final AsyncCallableCallback<Boolean> callback;

	public AddDocumentMessage(final String userID, final String key, final String json, final long created, final AddPersistenceLevel persistenceLevel, final AsyncCallableCallback<Boolean> callback)
	{
		super(userID, key);
		this.json = json;
		this.created = created;
		this.persistenceLevel = persistenceLevel;
		this.callback = callback;
	}

	public String getJson()
	{
		return json;
	}

	public long getCreated()
	{
		return created;
	}

	public AddPersistenceLevel getPersistenceLevel()
	{
		return persistenceLevel;
	}

	public AsyncCallableCallback<Boolean> getCallback()
	{
		return callback;
	}

	public void setResult(boolean result)
	{
		if (callback != null)
		{
			callback.setResult(result);
		}
	}
}
