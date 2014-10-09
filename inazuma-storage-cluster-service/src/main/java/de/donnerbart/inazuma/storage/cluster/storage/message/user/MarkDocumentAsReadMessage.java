package de.donnerbart.inazuma.storage.cluster.storage.message.user;

public class MarkDocumentAsReadMessage extends UserMessageWithKey
{
	public MarkDocumentAsReadMessage(final String userID, final String key)
	{
		super(userID, key);
	}
}
