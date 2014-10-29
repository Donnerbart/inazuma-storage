package de.donnerbart.inazuma.storage.base.request.task;

import de.donnerbart.inazuma.storage.base.request.DeletePersistenceLevel;
import de.donnerbart.inazuma.storage.base.request.RequestController;

import java.util.concurrent.Callable;

public class DeleteDocumentTask implements Callable<Boolean>
{
	private final String userID;
	private final String key;
	private final DeletePersistenceLevel deletePersistenceLevel;

	public DeleteDocumentTask(final String userID, final String key, final DeletePersistenceLevel deletePersistenceLevel)
	{
		this.userID = userID;
		this.key = key;
		this.deletePersistenceLevel = deletePersistenceLevel;
	}

	@Override
	public Boolean call()
	{
		//System.out.println("Delete document for user " + userID + " with key " + key);

		RequestController.getStorageControllerInstance().deleteDocument(userID, key, deletePersistenceLevel);

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

	public DeletePersistenceLevel getDeletePersistenceLevel()
	{
		return deletePersistenceLevel;
	}
}
