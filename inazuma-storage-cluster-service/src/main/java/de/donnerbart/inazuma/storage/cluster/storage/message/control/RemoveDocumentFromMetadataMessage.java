package de.donnerbart.inazuma.storage.cluster.storage.message.control;

public class RemoveDocumentFromMetadataMessage implements ControlMessage
{
	private final String id;

	public RemoveDocumentFromMetadataMessage(final String id)
	{
		this.id = id;
	}

	public String getId()
	{
		return id;
	}
}
