package de.donnerbart.inazuma.storage.cluster.storage.wrapper;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.document.StringDocument;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.response.DatabaseGetResponse;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.response.DatabaseResponse;
import rx.Observable;

public class CouchbaseWrapper implements DatabaseWrapper
{
	private final AsyncBucket bucket;

	public CouchbaseWrapper(final AsyncBucket bucket)
	{
		this.bucket = bucket;
	}

	public Observable<DatabaseResponse> getDocument(final String id)
	{
		return bucket.get(id, StringDocument.class).map(document -> new DatabaseGetResponse(document.content()));
	}

	public Observable<DatabaseResponse> insertDocument(final String key, final String json)
	{
		return bucket.upsert(StringDocument.create(key, json)).map(document -> null);
	}

	public Observable<DatabaseResponse> deleteDocument(final String id)
	{
		return bucket.remove(id).map(document -> null);
	}
}
