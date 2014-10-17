package de.donnerbart.inazuma.storage.cluster.storage.wrapper;

import rx.Observable;

public interface DatabaseWrapper
{
	public Observable<String> getDocument(final String id);

	public Observable<Object> insertDocument(final String key, final String document);

	public Observable<Object> deleteDocument(final String id);
}
