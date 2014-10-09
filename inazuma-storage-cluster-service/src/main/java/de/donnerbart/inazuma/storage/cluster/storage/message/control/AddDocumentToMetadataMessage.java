package de.donnerbart.inazuma.storage.cluster.storage.message.control;

import de.donnerbart.inazuma.storage.cluster.storage.metadata.DocumentMetadata;

public class AddDocumentToMetadataMessage implements ControlMessage
{
	private final String id;
	private final DocumentMetadata metadata;

	public AddDocumentToMetadataMessage(final String id, final DocumentMetadata metadata)
	{
		this.id = id;
		this.metadata = metadata;
	}

	public String getId()
	{
		return id;
	}

	public DocumentMetadata getMetadata()
	{
		return metadata;
	}
}
