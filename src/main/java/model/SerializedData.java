package model;

import com.hazelcast.core.PartitionAware;

import java.io.Serializable;

public class SerializedData implements StatusMessageObject, PartitionAware, Serializable
{
	private final String userID;
	private final long created;
	private final String key;
	private final String document;

	private int tries;
	private Exception lastException;

	public SerializedData(final String userID, final long created, final String key, final String document)
	{
		this.userID = userID;
		this.created = created;
		this.key = key;
		this.document = document;

		tries = 0;
		lastException = null;
	}

	public String getUserID()
	{
		return userID;
	}

	public long getCreated()
	{
		return created;
	}

	public String getKey()
	{
		return key;
	}

	public String getDocument()
	{
		return document;
	}

	@Override
	public int getTries()
	{
		return tries;
	}

	@Override
	public void incrementTries()
	{
		this.tries++;
	}

	@Override
	public Exception getLastException()
	{
		return lastException;
	}

	@Override
	public void setLastException(Exception lastException)
	{
		this.lastException = lastException;
	}

	@Override
	public void resetStatus()
	{
		tries = 0;
		lastException = null;
	}

	@Override
	public Object getPartitionKey()
	{
		return userID;
	}
}
