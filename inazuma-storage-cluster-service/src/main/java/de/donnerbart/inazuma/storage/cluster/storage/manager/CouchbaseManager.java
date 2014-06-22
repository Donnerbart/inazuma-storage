package de.donnerbart.inazuma.storage.cluster.storage.manager;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;

public class CouchbaseManager
{
	private static final Cluster cluster;
	private static final Bucket bucket;

	static
	{
		cluster = new CouchbaseCluster();
		bucket = cluster.openBucket("default").toBlocking().single();
	}

	public static Bucket getBucket()
	{
		return bucket;
	}

	public static void shutdown()
	{
		cluster.disconnect();
	}
}
