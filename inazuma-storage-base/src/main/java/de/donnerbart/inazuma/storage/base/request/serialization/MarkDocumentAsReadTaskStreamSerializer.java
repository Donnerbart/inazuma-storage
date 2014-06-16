package de.donnerbart.inazuma.storage.base.request.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import de.donnerbart.inazuma.storage.base.request.task.MarkDocumentAsReadTask;

import java.io.IOException;

public class MarkDocumentAsReadTaskStreamSerializer implements StreamSerializer<MarkDocumentAsReadTask>
{
	@Override
	public void write(final ObjectDataOutput out, final MarkDocumentAsReadTask object) throws IOException
	{
		out.writeUTF(object.getUserID());
		out.writeUTF(object.getKey());
	}

	@Override
	public MarkDocumentAsReadTask read(final ObjectDataInput in) throws IOException
	{
		return new MarkDocumentAsReadTask(in.readUTF(), in.readUTF());
	}

	@Override
	public int getTypeId()
	{
		return StreamSerializerId.MARK_DOCUMENT_AS_READ_TASK.ordinal();
	}

	@Override
	public void destroy()
	{
	}
}
