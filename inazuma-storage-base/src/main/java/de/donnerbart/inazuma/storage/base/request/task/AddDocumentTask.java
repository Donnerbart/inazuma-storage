package de.donnerbart.inazuma.storage.base.request.task;

import de.donnerbart.inazuma.storage.base.request.RequestController;

import java.util.concurrent.Callable;

public class AddDocumentTask implements Callable<Boolean>
{
	private final String userID;
	private final String key;
	private final String json;
	private final long created;

	public AddDocumentTask(final String userID, final String key, final String json, final long created)
	{
		this.userID = userID;
		this.key = key;
		this.json = json;
		this.created = created;
	}

	@Override
	public Boolean call()
	{
		//System.out.println("Add document for user " + userID + " with key " + key);

		RequestController.getStorageControllerInstance().addDocumentAsync(userID, key, json, created);

		return true;
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
}
