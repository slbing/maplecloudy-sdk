package com.maplecloudy.flume.sink;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogSchemaSource {

	private static LogSchemaSource mLogSchemaSource = null;

	public LogSchemaSource(Context context) {

	}

	public static LogSchemaSource getInstance(Context context) {
		if (mLogSchemaSource == null) {
			mLogSchemaSource = new LogSchemaSource(context);
		}
		return mLogSchemaSource;
	}

	AvroFileSerializer avroSerializer = null;

	public AvroFileSerializer getAvroSerializer(String schemaClass) throws IOException, ClassNotFoundException {
		if (avroSerializer != null)
			return avroSerializer;
		Schema schema = ReflectData.get().getSchema(Class.forName(schemaClass));
		avroSerializer = new AvroFileSerializer(schema);
		return avroSerializer;
	}

}
