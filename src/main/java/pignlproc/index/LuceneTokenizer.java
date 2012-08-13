package pignlproc.index;




import org.apache.lucene.analysis.Analyzer;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.util.Version;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 /**
 * @author chrishokamp
 * Take a bag of paragraphs (usage contexts) and emit a bag of tokens using the Lucene analyzer
 *
 */
public class LuceneTokenizer extends EvalFunc<DataBag> {
    TupleFactory tupleFactory = TupleFactory.getInstance();
    BagFactory bagFactory = BagFactory.getInstance();
    //String language;

    //Hard-coded for the Lucene analyzer because this is unnecessary for this implementation
    String field = "paragraph";

    private String stoplist_path; //the path to the stoplist
    private String stoplist_name; //the name of the stoplist
    private HashSet<String> stopset = null;
    protected Analyzer analyzer;
    private TokenStream stream = null;

    public LuceneTokenizer(String path, String name) throws  IOException {
        stoplist_path = path;
        stoplist_name = name;
    }

    public List<String> getCacheFiles() {
        List<String> list = new ArrayList<String>(1);
        list.add(stoplist_path);
        return list;
    }

    @Override
    public DataBag exec(Tuple input) throws IOException {

        if (stopset == null)
        {
            //uses hadoop distributed cache (via getCacheFiles)
            FileReader fr = new FileReader("./" + stoplist_name);
            stopset = new HashSet<String>();
            BufferedReader br = new BufferedReader(fr);
            String line = null;
            while ((line = br.readLine()) != null)
            {
                stopset.add(line);
            }

            analyzer = new EnglishAnalyzer(Version.LUCENE_36, stopset);
        }

        DataBag out = bagFactory.newDefaultBag();

        Object t0 = input.get(0);
        if (!(t0 instanceof String)) {
          throw new IOException(
                 "Expected a String, but this is a "
                         + t0.getClass().getName());
         }

        stream = analyzer.reusableTokenStream(field, new StringReader((String)t0));

        try {
            while (stream.incrementToken()) {
                String token = stream.getAttribute(CharTermAttribute.class).toString();
                out.add(tupleFactory.newTuple(token));
            }
        }
        catch (IOException e) {
               throw e;
        }
        return out;

    }

    public Schema outputSchema(Schema input) {
        try {
            Schema tupleSchema = new Schema();
            tupleSchema.add(new FieldSchema("token", DataType.CHARARRAY));
            return new Schema(new Schema.FieldSchema(getSchemaName(
                    this.getClass().getName().toLowerCase(), input),
                    tupleSchema, DataType.BAG));
        } catch (Exception e) {
            return null;
        }
    }

}






