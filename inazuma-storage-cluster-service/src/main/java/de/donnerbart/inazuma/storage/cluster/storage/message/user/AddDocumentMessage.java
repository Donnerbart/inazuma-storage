package de.donnerbart.inazuma.storage.cluster.storage.message.user;

public class AddDocumentMessage extends UserMessageWithKey
{
	private final String json;
	private final long created;

	public AddDocumentMessage(final String userID, final String key, final String json, final long created)
	{
		super(userID, key);
		this.json = json;
		this.created = created;
	}

	public String getJson()
	{
		return json;
	}

	public long getCreated()
	{
		return created;
	}
}