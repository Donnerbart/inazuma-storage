package de.donnerbart.inazuma.storage.cluster.storage.wrapper;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.StringDocument;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.response.DatabaseGetResponse;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.response.DatabaseResponse;
import rx.Observable;

public class CouchbaseWrapper implements DatabaseWrapper
{
	private final Bucket bucket;

	public CouchbaseWrapper(final Bucket bucket)
	{
		this.bucket = bucket;
	}

	public Observable<DatabaseResponse> getDocument(final String id)
	{
		return bucket.async().get(id, StringDocument.class).map(document -> new DatabaseGetResponse(document.content()));
	}

	public Observable<DatabaseResponse> insertDocument(final String key, final String json)
	{
		return bucket.async().upsert(StringDocument.create(key, json)).map(document -> null);
	}

	public Observable<DatabaseResponse> deleteDocument(final String id)
	{
		return bucket.async().remove(id).map(document -> null);
	}
}
