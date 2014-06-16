package de.donnerbart.inazuma.storage.cluster.storage.message;

import com.hazelcast.core.PartitionAware;

import java.io.Serializable;

public class BaseMessage implements PartitionAware, Serializable
{
	private final MessageType type;
	private final String userID;

	public BaseMessage(final MessageType type, final String userID)
	{
		this.type = type;
		this.userID = userID;
	}

	public MessageType getType()
	{
		return type;
	}

	public String getUserID()
	{
		return userID;
	}

	@Override
	public Object getPartitionKey()
	{
		return userID;
	}
}
