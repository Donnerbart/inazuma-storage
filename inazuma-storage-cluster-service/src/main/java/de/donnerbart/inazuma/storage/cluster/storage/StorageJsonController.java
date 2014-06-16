package de.donnerbart.inazuma.storage.cluster.storage;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import de.donnerbart.inazuma.storage.cluster.storage.model.DocumentMetadata;
import de.donnerbart.inazuma.storage.cluster.storage.model.DocumentMetadataAdapter;

import java.lang.reflect.Type;
import java.util.Map;

class StorageJsonController
{
	private static final Type typeOfMap;

	private static final Gson gson;

	static
	{
		typeOfMap = new TypeToken<Map<String, DocumentMetadata>>()
		{
		}.getType();

		final GsonBuilder builder = new GsonBuilder();
		builder.registerTypeAdapter(DocumentMetadata.class, new DocumentMetadataAdapter());

		gson = builder.create();
	}

	public static Map<String, DocumentMetadata> getDocumentMetadataMap(final String json)
	{
		return gson.fromJson(json, typeOfMap);
	}

	public static String toJson(final Map<String, DocumentMetadata> documentMetadataMap)
	{
		return gson.toJson(documentMetadataMap, typeOfMap);
	}
}
