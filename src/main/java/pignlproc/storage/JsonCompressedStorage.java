package pignlproc.storage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.builtin.JsonStorage;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;

import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreMetadata;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;


/**
 * @author Chris Hokamp
 */
public class JsonCompressedStorage extends JsonStorage {

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        job.getConfiguration().set("mapred.textoutputformat.separator", "");
        FileOutputFormat.setOutputPath(job, new Path(location));

        if( "true".equals( job.getConfiguration().get( "output.compression.enabled" ) ) ) {
            FileOutputFormat.setCompressOutput( job, true );
            String codec = job.getConfiguration().get( "output.compression.codec" );
            try {
                FileOutputFormat.setOutputCompressorClass( job,  (Class<? extends CompressionCodec>) Class.forName( codec ) );
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Class not found: " + codec );
            }
        } else {
            // This makes it so that storing to a directory ending with ".gz" or ".bz2" works.
            setCompression(new Path(location), job);
        }
    }

    private void setCompression(Path path, Job job) {
        String location=path.getName();
        if (location.endsWith(".bz2") || location.endsWith(".bz")) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job,  BZip2Codec.class);
        }  else if (location.endsWith(".gz")) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        } else {
            FileOutputFormat.setCompressOutput( job, false);
        }
    }



}
