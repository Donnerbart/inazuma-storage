package request.task;

import inazuma.InazumaStorageManager;

import java.util.concurrent.Callable;

public class GetDocumentMetadataTask implements Callable<String>
{
	private final String userID;

	public GetDocumentMetadataTask(final String userID)
	{
		this.userID = userID;
	}

	@Override
	public String call() throws Exception
	{
		//System.out.println("Get document metadata for user " + userID);

		return InazumaStorageManager.getStorageController().getDocumentMetadata(userID);
	}

	public String getUserID()
	{
		return userID;
	}
}
