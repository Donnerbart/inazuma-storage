package de.donnerbart.inazuma.storage.cluster.storage.message.control;

public class PersistDocumentMetadataMessage implements ControlMessage
{
	private static final PersistDocumentMetadataMessage INSTANCE = new PersistDocumentMetadataMessage();

	public static PersistDocumentMetadataMessage getInstance()
	{
		return INSTANCE;
	}
}
