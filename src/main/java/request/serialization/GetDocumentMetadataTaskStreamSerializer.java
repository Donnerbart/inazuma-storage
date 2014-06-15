package request.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import request.task.GetDocumentMetadataTask;

import java.io.IOException;

public class GetDocumentMetadataTaskStreamSerializer implements StreamSerializer<GetDocumentMetadataTask>
{
	@Override
	public void write(final ObjectDataOutput out, final GetDocumentMetadataTask object) throws IOException
	{
		out.writeUTF(object.getUserID());
	}

	@Override
	public GetDocumentMetadataTask read(final ObjectDataInput in) throws IOException
	{
		return new GetDocumentMetadataTask(in.readUTF());
	}

	@Override
	public int getTypeId()
	{
		return StreamSerializerId.GET_DOCUMENT_METADATA_TASK.ordinal();
	}

	@Override
	public void destroy()
	{
	}
}
