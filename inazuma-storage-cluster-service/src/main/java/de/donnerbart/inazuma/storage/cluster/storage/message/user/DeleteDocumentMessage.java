package de.donnerbart.inazuma.storage.cluster.storage.message.user;

public class DeleteDocumentMessage extends UserMessageWithKey
{
	public DeleteDocumentMessage(final String userID, final String key)
	{
		super(userID, key);
	}
}
