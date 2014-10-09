package de.donnerbart.inazuma.storage.cluster.storage.message.user;

public class FetchDocumentMessage extends UserCallbackMessage<String>
{
	private final String key;

	public FetchDocumentMessage(final String userID, final String key)
	{
		super(userID);
		this.key = key;
	}

	public String getKey()
	{
		return key;
	}
}
