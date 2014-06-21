package de.donnerbart.inazuma.storage.cluster.storage.model;

public class DocumentMetadataUtil
{
	public static String createKeyFromUserID(final String userID)
	{
		return "u-" + userID;
	}
}
