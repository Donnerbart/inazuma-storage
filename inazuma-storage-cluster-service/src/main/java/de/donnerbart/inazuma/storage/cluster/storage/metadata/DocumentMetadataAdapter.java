package de.donnerbart.inazuma.storage.cluster.storage.metadata;

import com.google.gson.*;

import java.lang.reflect.Type;

public class DocumentMetadataAdapter implements JsonSerializer<DocumentMetadata>, JsonDeserializer<DocumentMetadata>
{
	@Override
	public JsonElement serialize(final DocumentMetadata documentMetadata, final Type type, final JsonSerializationContext jsonSerializationContext)
	{
		final JsonObject object = new JsonObject();

		object.addProperty("c", documentMetadata.getCreated());
		object.addProperty("r", documentMetadata.isRead() ? 1 : 0);

		return object;
	}

	@Override
	public DocumentMetadata deserialize(final JsonElement jsonElement, final Type type, final JsonDeserializationContext jsonDeserializationContext) throws JsonParseException
	{
		final JsonObject object = jsonElement.getAsJsonObject();
		return new DocumentMetadata(
				object.get("c").getAsLong(),
				object.get("r").getAsBoolean()
		);
	}
}
