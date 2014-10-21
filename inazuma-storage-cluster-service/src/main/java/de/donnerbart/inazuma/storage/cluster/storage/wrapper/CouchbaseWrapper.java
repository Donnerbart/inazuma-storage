package de.donnerbart.inazuma.storage.cluster.storage.wrapper;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.document.StringDocument;
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
				.get(id, StringDocument.class)
				.singleOrDefault(null)
				.map(document -> {
					if (document == null)
					{
						return null;
					}
					return document.content();
				});
	}

	public Observable<Object> insertDocument(final String key, final String json)
	{
		return bucket
				.upsert(StringDocument.create(key, json))
				.map(document -> null);
	}

	public Observable<Object> deleteDocument(final String id)
	{
		return bucket
				.remove(id)
				.map(document -> null);
	}
}
