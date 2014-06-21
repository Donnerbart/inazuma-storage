package de.donnerbart.inazuma.storage.cluster.storage.wrapper;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.StringDocument;

import java.util.concurrent.ExecutionException;

public class CouchbaseWrapper
{
	private final Bucket bucket;

	public CouchbaseWrapper(final Bucket bucket)
	{
		this.bucket = bucket;
	}

	public String getDocument(final String id)
	{
		//final StringDocument document = bucket.get(id, StringDocument.class).toBlocking().single();
		//return (document != null) ? document.content() : null;
		return bucket.get(id, StringDocument.class).toBlocking().single().content();
	}

	public void insertDocument(final String key, final String document)
	{
		bucket.upsert(StringDocument.create(key, document)).toBlocking().single();
	}

	public void deleteDocument(final String key) throws ExecutionException, InterruptedException
	{
		bucket.remove(key);
	}
}
