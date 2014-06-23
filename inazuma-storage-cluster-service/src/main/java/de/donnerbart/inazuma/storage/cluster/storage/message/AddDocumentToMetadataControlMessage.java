package de.donnerbart.inazuma.storage.cluster.storage.message;

import de.donnerbart.inazuma.storage.cluster.storage.metadata.DocumentMetadata;

public class AddDocumentToMetadataControlMessage extends ControlMessage
{
	private final DocumentMetadata metadata;

	public AddDocumentToMetadataControlMessage(final String id, final DocumentMetadata metadata)
	{
		super(ControlMessageType.ADD_DOCUMENT_TO_METADATA, id);
		this.metadata = metadata;
	}

	public DocumentMetadata getMetadata()
	{
		return metadata;
	}
}
