package com.maplecloudy.avro.util;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.maplecloudy.avro.io.MapAvroFile;
import com.maplecloudy.avro.io.Pair;

/** Reads a data file to get its schema. */
public class CutDataTool implements Tool {

	@Override
	public String getName() {
		return "cut";
	}

	@Override
	public String getShortDescription() {
		return "Get the specific number of head record as example data.";
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public int run(InputStream stdin, PrintStream out, PrintStream err,
			List<String> args) throws Exception {
		if (args.size() < 1) {
			err.println("Usage: input_file [-n num defalt 10] [-o output path]");
			return 1;
		}
		Path inputFile = new Path(args.get(0));
		int num = 10;
		Path output = new Path("out");
		for (int i = 0; i < args.size(); i++) {
			if ("-n".equals(args.get(i))) {
				num = Integer.parseInt(args.get(i + 1));
				i++;
			}
			if ("-o".equals(args.get(i))) {
				output = new Path(args.get(i + 1));
				i++;
			}
		}

		Configuration conf = new Configuration();
		conf.setClass(MapAvroFile.Reader.DATUM_READER_CLASS,
				GenericDatumReader.class, DatumReader.class);
		FileSystem fs = FileSystem.get(conf);
		Path input = new Path(args.get(0));
		Path datafile = new Path(input, MapAvroFile.DATA_FILE_NAME);
		Path indexfile = new Path(input, MapAvroFile.INDEX_FILE_NAME);
		if (fs.exists(datafile) && fs.exists(indexfile)) {
			MapAvroFile.Reader reader = null;
			MapAvroFile.Writer writer = null;
			try {
				// readers = new
				reader = new MapAvroFile.Reader(fs, input.toString(), conf);
				writer = new MapAvroFile.Writer(conf, fs, output.toString(),
						reader.getKeySchema(), reader.getValueSchema());
				while (num > 0 && reader.hasNext()) {
					Pair pair = reader.next();
					writer.append(pair.key(), pair.value());
					num--;
				}
			} finally {
				if (reader != null)
					reader.close();
				if (writer != null)
					writer.close();
			}
		} else {
			DataFileReader reader = null;
			DataFileWriter writer = null;
			try {
				reader = AvroUtils.Open(conf, inputFile);
				writer = AvroUtils.Create(conf, output, reader.getSchema());
				while (num > 0 && reader.hasNext()) {
					writer.append(reader.next());
					num--;
				}
			} finally {
				if (reader != null)
					reader.close();
				if (writer != null)
					writer.close();
			}
		}
		err.println("导出完成!");
		return 0;
	}
}
