package de.donnerbart.inazuma.storage.cluster;

import de.donnerbart.inazuma.storage.cluster.storage.manager.CouchbaseManager;

public class RemoveTestBucket
{
	public static void main(final String[] args)
	{
		final String username = System.getProperty("username", "Administrator");
		final String password = System.getProperty("password", "");
		final String bucketName = System.getProperty("bucket.name", "inazuma-test");

		CouchbaseManager.removeBucket(username, password, bucketName);
	}
}
