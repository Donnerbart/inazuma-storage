package de.donnerbart.inazuma.storage.cluster.storage.wrapper.response;

public class DatabaseGetResponse implements DatabaseResponse
{
	private final String content;

	public DatabaseGetResponse(final String content)
	{
		this.content = content;
	}

	public String getContent()
	{
		return content;
	}
}
