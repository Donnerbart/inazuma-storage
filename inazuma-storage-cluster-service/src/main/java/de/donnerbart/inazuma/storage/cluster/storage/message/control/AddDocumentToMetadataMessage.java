package de.donnerbart.inazuma.storage.cluster.storage.message.control;

import de.donnerbart.inazuma.storage.cluster.storage.metadata.DocumentMetadata;

public class AddDocumentToMetadataMessage implements ControlMessage
{
	private final String key;
	private final DocumentMetadata metadata;

	public AddDocumentToMetadataMessage(final String key, final DocumentMetadata metadata)
	{
		this.key = key;
		this.metadata = metadata;
	}

	public String getKey()
	{
		return key;
	}

	public DocumentMetadata getMetadata()
	{
		return metadata;
	}
}
