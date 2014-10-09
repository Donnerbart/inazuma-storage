package de.donnerbart.inazuma.storage.cluster.storage.message.control;

public class LoadDocumentMetadataMessage implements ControlMessage
{
	private static final LoadDocumentMetadataMessage INSTANCE = new LoadDocumentMetadataMessage();

	public static LoadDocumentMetadataMessage getInstance()
	{
		return INSTANCE;
	}
}
