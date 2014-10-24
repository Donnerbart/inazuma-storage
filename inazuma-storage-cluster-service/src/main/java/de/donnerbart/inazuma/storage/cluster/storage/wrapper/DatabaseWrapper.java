package de.donnerbart.inazuma.storage.cluster.storage.wrapper;

import rx.Observable;

public interface DatabaseWrapper
{
	public Observable<String> getDocument(final String id);

	public Observable<Boolean> insertDocument(final String key, final String document);

	public Observable<Boolean> deleteDocument(final String id);
}
