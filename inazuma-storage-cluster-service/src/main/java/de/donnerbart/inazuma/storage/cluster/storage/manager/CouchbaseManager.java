package de.donnerbart.inazuma.storage.cluster.storage.manager;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;

public class CouchbaseManager
{
	private static final Cluster cluster;

	static
	{
		cluster = CouchbaseCluster.create();
	}

	public static AsyncBucket getAsyncBucket(final String name)
	{
		return cluster.openBucket(name).async();
	}

	public static void shutdown()
	{
		cluster.disconnect();
	}
}
