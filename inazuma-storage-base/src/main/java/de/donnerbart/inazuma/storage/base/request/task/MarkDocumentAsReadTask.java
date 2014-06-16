package de.donnerbart.inazuma.storage.base.request.task;

import de.donnerbart.inazuma.storage.base.request.RequestController;

import java.util.concurrent.Callable;

public class MarkDocumentAsReadTask implements Callable<Boolean>
{
	private final String userID;
	private final String key;

	public MarkDocumentAsReadTask(final String userID, final String key)
	{
		this.userID = userID;
		this.key = key;
	}

	@Override
	public Boolean call()
	{
		//System.out.println("Mark document as read for user " + userID + " with key " + key);

		RequestController.getStorageControllerInstance().markDocumentAsReadAsync(userID, key);

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
}
