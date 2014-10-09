package de.donnerbart.inazuma.storage.cluster.storage.message.user;

public class FetchDocumentMetadataMessage extends UserCallbackMessage<String>
{
	public FetchDocumentMetadataMessage(final String userID)
	{
		super(userID);
	}
}
