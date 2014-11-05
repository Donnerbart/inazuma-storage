package de.donnerbart.inazuma.storage.cluster.storage.wrapper;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.document.RawJsonDocument;
import rx.Observable;

public class CouchbaseWrapper implements DatabaseWrapper
{
	private final AsyncBucket bucket;

	public CouchbaseWrapper(final AsyncBucket bucket)
	{
		this.bucket = bucket;
	}

	public Observable<String> getDocument(final String id)
	{
		return bucket
				.get(id, RawJsonDocument.class)
				.singleOrDefault(null)
				.map(document -> {
					if (document == null)
					{
						return null;
					}
					return document.content();
				});
	}

	public Observable<Boolean> insertDocument(final String key, final String json)
	{
		return bucket
				.upsert(RawJsonDocument.create(key, json))
				.map(document -> Boolean.TRUE);
	}

	public Observable<Boolean> deleteDocument(final String id)
	{
		return bucket
				.remove(id)
				.map(document -> Boolean.TRUE);
	}
}
