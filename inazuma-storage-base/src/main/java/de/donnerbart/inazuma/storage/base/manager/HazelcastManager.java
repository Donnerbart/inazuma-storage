package de.donnerbart.inazuma.storage.base.manager;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import de.donnerbart.inazuma.storage.base.request.serialization.AddDocumentTaskStreamSerializer;
import de.donnerbart.inazuma.storage.base.request.serialization.DeleteDocumentTaskStreamSerializer;
import de.donnerbart.inazuma.storage.base.request.serialization.GetDocumentMetadataTaskStreamSerializer;
import de.donnerbart.inazuma.storage.base.request.serialization.GetDocumentTaskStreamSerializer;
import de.donnerbart.inazuma.storage.base.request.task.AddDocumentTask;
import de.donnerbart.inazuma.storage.base.request.task.DeleteDocumentTask;
import de.donnerbart.inazuma.storage.base.request.task.GetDocumentMetadataTask;
import de.donnerbart.inazuma.storage.base.request.task.GetDocumentTask;

public class HazelcastManager
{
	public static HazelcastInstance getClientInstance()
	{
		final ClientConfig cfg = new ClientConfig();
		addSerializationConfig(cfg.getSerializationConfig());

		return HazelcastClient.newHazelcastClient(cfg);
	}

	public static HazelcastInstance getInstance()
	{
		final Config cfg = new Config();
		addSerializationConfig(cfg.getSerializationConfig());

		return Hazelcast.newHazelcastInstance(cfg);
	}

	private static void addSerializationConfig(final SerializationConfig cfg)
	{
		final SerializerConfig addDocumentConfig = new SerializerConfig();
		addDocumentConfig.setImplementation(new AddDocumentTaskStreamSerializer()).setTypeClass(AddDocumentTask.class);

		final SerializerConfig deleteDocumentConfig = new SerializerConfig();
		deleteDocumentConfig.setImplementation(new DeleteDocumentTaskStreamSerializer()).setTypeClass(DeleteDocumentTask.class);

		final SerializerConfig getDocumentConfig = new SerializerConfig();
		getDocumentConfig.setImplementation(new GetDocumentTaskStreamSerializer()).setTypeClass(GetDocumentTask.class);

		final SerializerConfig getDocumentMetadataConfig = new SerializerConfig();
		getDocumentMetadataConfig.setImplementation(new GetDocumentMetadataTaskStreamSerializer()).setTypeClass(GetDocumentMetadataTask.class);

		cfg
				.addSerializerConfig(addDocumentConfig)
				.addSerializerConfig(deleteDocumentConfig)
				.addSerializerConfig(getDocumentConfig)
				.addSerializerConfig(getDocumentMetadataConfig);
	}
}
