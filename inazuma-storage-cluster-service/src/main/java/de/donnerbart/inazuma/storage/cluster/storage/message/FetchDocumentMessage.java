package de.donnerbart.inazuma.storage.cluster.storage.message;

public class FetchDocumentMessage extends BaseCallbackMessageWithKey<String>
{
	public FetchDocumentMessage(final String userID, final String key)
	{
		super(MessageType.FETCH_DOCUMENT, userID, key);
	}
}
