package de.donnerbart.inazuma.storage.cluster.storage.message;

public class FetchDocumentMetadataMessage extends BaseCallbackMessage<String>
{
	public FetchDocumentMetadataMessage(final String userID)
	{
		super(MessageType.FETCH_DOCUMENT_METADATA, userID);
	}
}
