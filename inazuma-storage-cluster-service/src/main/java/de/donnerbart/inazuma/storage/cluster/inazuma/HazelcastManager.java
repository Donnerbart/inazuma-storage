package de.donnerbart.inazuma.storage.cluster.inazuma;

import com.hazelcast.config.Config;
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
	public static HazelcastInstance getInstance()
	{
		final SerializerConfig addDocumentConfig = new SerializerConfig();
		addDocumentConfig.setImplementation(new AddDocumentTaskStreamSerializer()).setTypeClass(AddDocumentTask.class);

		final SerializerConfig deleteDocumentConfig = new SerializerConfig();
		deleteDocumentConfig.setImplementation(new DeleteDocumentTaskStreamSerializer()).setTypeClass(DeleteDocumentTask.class);

		final SerializerConfig getDocumentConfig = new SerializerConfig();
		getDocumentConfig.setImplementation(new GetDocumentTaskStreamSerializer()).setTypeClass(GetDocumentTask.class);

		final SerializerConfig getDocumentMetadataConfig = new SerializerConfig();
		getDocumentMetadataConfig.setImplementation(new GetDocumentMetadataTaskStreamSerializer()).setTypeClass(GetDocumentMetadataTask.class);

		final Config cfg = new Config();
		cfg.getSerializationConfig()
				.addSerializerConfig(addDocumentConfig)
				.addSerializerConfig(deleteDocumentConfig)
				.addSerializerConfig(getDocumentConfig)
				.addSerializerConfig(getDocumentMetadataConfig);

		return Hazelcast.newHazelcastInstance(cfg);
	}
}
