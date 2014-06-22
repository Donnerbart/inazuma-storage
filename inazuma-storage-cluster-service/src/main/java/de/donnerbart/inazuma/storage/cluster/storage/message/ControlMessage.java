package de.donnerbart.inazuma.storage.cluster.storage.message;

import java.util.HashMap;
import java.util.Map;

public class ControlMessage
{
	private final static Map<ControlMessageType, ControlMessage> CACHED_MESSAGES = new HashMap<>();

	private final ControlMessageType type;
	private final String content;

	static
	{
		for (final ControlMessageType type : ControlMessageType.values())
		{
			CACHED_MESSAGES.put(type, new ControlMessage(type, null));
		}
	}

	private ControlMessage(final ControlMessageType type, final String content)
	{
		this.type = type;
		this.content = content;
	}

	public static ControlMessage create(final ControlMessageType type)
	{
		return CACHED_MESSAGES.get(type);
	}

	public static ControlMessage create(final ControlMessageType type, final String content)
	{
		return new ControlMessage(type, content);
	}

	public ControlMessageType getType()
	{
		return type;
	}

	public String getContent()
	{
		return content;
	}
}
