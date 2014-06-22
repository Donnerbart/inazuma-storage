package de.donnerbart.inazuma.storage.cluster.storage.wrapper;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.StringDocument;
import rx.Observable;

import java.util.concurrent.ExecutionException;

public class CouchbaseWrapper
{
	private final Bucket bucket;

	public CouchbaseWrapper(final Bucket bucket)
	{
		this.bucket = bucket;
	}

	public Observable<StringDocument> getDocument(final String id)
	{
		return bucket.get(id, StringDocument.class);
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
