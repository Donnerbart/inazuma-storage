package de.donnerbart.inazuma.storage.cluster.storage;

import de.donnerbart.inazuma.storage.base.stats.CustomStatisticValue;

class StorageQueueSizeCollector implements CustomStatisticValue.ValueCollector<Long>
{
	private final StorageController storageController;

	public StorageQueueSizeCollector(final StorageController storageController)
	{
		this.storageController = storageController;
	}

	@Override
	public Long collectValue()
	{
		return storageController.getQueueSize();
	}

	@Override
	public String getType()
	{
		return "java.lang.Long";
	}
}
