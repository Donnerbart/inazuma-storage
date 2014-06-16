package de.donnerbart.inazuma.storage.base.request.task;

import de.donnerbart.inazuma.storage.base.request.RequestController;

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

		return RequestController.getStorageControllerInstance().getDocumentMetadata(userID);
	}

	public String getUserID()
	{
		return userID;
	}
}
