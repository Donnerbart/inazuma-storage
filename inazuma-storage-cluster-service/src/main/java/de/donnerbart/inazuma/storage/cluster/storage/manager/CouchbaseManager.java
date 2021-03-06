package de.donnerbart.inazuma.storage.cluster.storage.manager;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.DefaultBucketSettings;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class CouchbaseManager
{
	private static final Cluster cluster = CouchbaseCluster.create();

	public static AsyncBucket getAsyncBucket(final String name)
	{
		return cluster.openBucket(name).async();
	}

	public static void createBucket(final String username, final String password, final String name, final int quota, final boolean enableFlush)
	{
		final BucketSettings bucketSettings = DefaultBucketSettings
				.builder()
				.name(name)
				.quota(quota)
				.enableFlush(enableFlush)
				.build();

		cluster
				.clusterManager(username, password)
				.insertBucket(bucketSettings);
	}

	public static void removeBucket(final String username, final String password, final String name)
	{
		cluster
				.clusterManager(username, password)
				.removeBucket(name);
	}

	public static boolean flushBucket(final String name)
	{
		return flushBucket(name, 10, TimeUnit.SECONDS);
	}

	public static boolean flushBucket(final String name, final long timeout, final TimeUnit timeUnit)
	{
		return cluster.openBucket(name)
				.bucketManager()
				.flush(timeout, timeUnit);
	}

	public static List<BucketSettings> getBucketSettings(final String username, final String password)
	{
		return cluster
				.clusterManager(username, password)
				.getBuckets();
	}

	public static void shutdown()
	{
		cluster.disconnect();
	}
}
