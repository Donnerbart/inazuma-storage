package de.donnerbart.inazuma.storage.cluster.storage.manager;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;

public class CouchbaseManager
{
	private static final Cluster cluster;
	private static final AsyncBucket bucket;

	static
	{
		cluster = CouchbaseCluster.create();
		bucket = cluster.openBucket("default").async();
	}

	public static AsyncBucket getAsyncBucket()
	{
		return bucket;
	}

	public static void shutdown()
	{
		cluster.disconnect();
	}
}
