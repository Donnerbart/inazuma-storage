package request.task;

import request.RequestController;

import java.util.concurrent.Callable;

public class GetDocumentTask implements Callable<String>
{
	private final String userID;
	private final String key;

	public GetDocumentTask(final String userID, final String key)
	{
		this.userID = userID;
		this.key = key;
	}

	@Override
	public String call() throws Exception
	{
		//System.out.println("Get document for user " + userID + " with key " + key);

		return RequestController.getStorageControllerInstance().getDocument(userID, key);
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
