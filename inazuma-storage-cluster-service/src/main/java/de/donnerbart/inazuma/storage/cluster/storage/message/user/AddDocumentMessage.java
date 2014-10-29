package de.donnerbart.inazuma.storage.cluster.storage.message.user;

import de.donnerbart.inazuma.storage.base.request.AddPersistenceLevel;

public class AddDocumentMessage extends UserCallbackMessage<Boolean>
{
	private final String json;
	private final String key;
	private final long created;
	private final AddPersistenceLevel persistenceLevel;

	public AddDocumentMessage(final String userID, final String key, final String json, final long created, final AddPersistenceLevel persistenceLevel)
	{
		super(userID);
		this.json = json;
		this.key = key;
		this.created = created;
		this.persistenceLevel = persistenceLevel;
	}

	public String getJson()
	{
		return json;
	}

	public String getKey()
	{
		return key;
	}

	public long getCreated()
	{
		return created;
	}

	public AddPersistenceLevel getPersistenceLevel()
	{
		return persistenceLevel;
	}
}
