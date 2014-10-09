package de.donnerbart.inazuma.storage.cluster.storage.message.control;

public class RemoveDocumentFromMetadataMessage implements ControlMessage
{
	private final String key;

	public RemoveDocumentFromMetadataMessage(final String key)
	{
		this.key = key;
	}

	public String getKey()
	{
		return key;
	}
}
