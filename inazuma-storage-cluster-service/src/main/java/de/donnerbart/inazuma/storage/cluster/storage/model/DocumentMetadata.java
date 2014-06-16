package de.donnerbart.inazuma.storage.cluster.storage.model;

import de.donnerbart.inazuma.storage.cluster.storage.message.AddDocumentMessage;

public class DocumentMetadata
{
	private final long created;
	private boolean read;

	public DocumentMetadata(final AddDocumentMessage message)
	{
		this(message.getCreated(), false);
	}

	public DocumentMetadata(final long created, final boolean read)
	{
		this.created = created;
		this.read = read;
	}

	public long getCreated()
	{
		return created;
	}

	public boolean isRead()
	{
		return read;
	}

	public void setRead(final boolean read)
	{
		this.read = read;
	}
}
