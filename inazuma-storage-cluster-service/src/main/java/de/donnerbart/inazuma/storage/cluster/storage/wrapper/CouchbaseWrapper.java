package de.donnerbart.inazuma.storage.cluster.storage.wrapper;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.StringDocument;
import rx.Observable;

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

	public Observable<StringDocument> insertDocument(final String key, final String document)
	{
		return bucket.upsert(StringDocument.create(key, document));
	}

	public Observable<JsonDocument> deleteDocument(final String key)
	{
		return bucket.remove(key);
	}
}
