package inazuma;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import request.serialization.AddDocumentTaskStreamSerializer;
import request.serialization.DeleteDocumentTaskStreamSerializer;
import request.serialization.GetDocumentMetadataTaskStreamSerializer;
import request.serialization.GetDocumentTaskStreamSerializer;
import request.task.AddDocumentTask;
import request.task.DeleteDocumentTask;
import request.task.GetDocumentMetadataTask;
import request.task.GetDocumentTask;

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
