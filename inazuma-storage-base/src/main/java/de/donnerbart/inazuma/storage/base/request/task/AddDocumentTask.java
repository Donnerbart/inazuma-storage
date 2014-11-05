package de.donnerbart.inazuma.storage.base.request.task;

import com.hazelcast.core.AsyncCallable;
import de.donnerbart.inazuma.storage.base.request.AddPersistenceLevel;
import de.donnerbart.inazuma.storage.base.request.RequestController;

public class AddDocumentTask extends AsyncCallable<Boolean>
{
	private final String userID;
	private final String key;
	private final String json;
	private final long created;
	private final AddPersistenceLevel persistenceLevel;

	public AddDocumentTask(final String userID, final String key, final String json, final long created, final AddPersistenceLevel persistenceLevel)
	{
		this.userID = userID;
		this.key = key;
		this.json = json;
		this.created = created;
		this.persistenceLevel = persistenceLevel;
	}

	@Override
	protected void asyncCall()
	{
		//System.out.println("Add document for user " + userID + " with key " + key);

		RequestController.getStorageControllerInstance().addDocument(userID, key, json, created, persistenceLevel, this::setResult);
	}

	public String getUserID()
	{
		return userID;
	}

	public String getKey()
	{
		return key;
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
}
