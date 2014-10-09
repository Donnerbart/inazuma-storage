package de.donnerbart.inazuma.storage.cluster.storage.message.control;

public class CreateMetadataDocumentMessage implements ControlMessage
{
	private final String json;

	public CreateMetadataDocumentMessage(final String json)
	{
		this.json = json;
	}

	public String getJson()
	{
		return json;
	}
}
