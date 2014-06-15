package request.task;

import inazuma.InazumaStorageManager;

import java.util.concurrent.Callable;

public class DeleteDocumentTask implements Callable<Boolean>
{
	private final String userID;
	private final String key;

	public DeleteDocumentTask(final String userID, final String key)
	{
		this.userID = userID;
		this.key = key;
	}

	@Override
	public Boolean call()
	{
		//System.out.println("Delete document for user " + userID + " with key " + key);

		InazumaStorageManager.getStorageController().deleteDocumentAsync(userID, key);

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
