package pignlproc.index;




import org.apache.lucene.analysis.Analyzer;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
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
 * Take a bag of paragraphs (usage contexts) and emit a bag of tokens using the Lucene Standard analyzer
 *
 */
public class LuceneTokenizer extends EvalFunc<DataBag> {
    TupleFactory tupleFactory = TupleFactory.getInstance();
    BagFactory bagFactory = BagFactory.getInstance();
    //Hard-coded for the Lucene analyzer because this is unnecessary for this implementation
    String field = "paragraph";

    //TODO: how to read the stopwords file?? - testing in constructor
    protected Analyzer analyzer;
    private TokenStream stream = null;

    final String stoplist;
    final HashSet<String> stopset;


    public LuceneTokenizer(String path) throws  IOException {
        stoplist = path;
        getCacheFiles(); //TODO: see if this works - hackish implementation

        stopset = new HashSet<String>();
        //uses hadoop distributed cache (via getCacheFiles)
        FileReader fr = new FileReader("./stopwords.en.list");

        BufferedReader br = new BufferedReader(fr);
        String line = null;
        while ((line = br.readLine()) != null)
        {
            stopset.add(line);
        }

        analyzer =  new EnglishAnalyzer(Version.LUCENE_36, stopset);
        stream = analyzer.reusableTokenStream(field, new StringReader(""));
    }


    @Override
    public DataBag exec(Tuple input) throws IOException {

            DataBag out = bagFactory.newDefaultBag();

            Object t0 = input.get(0);
            if (!(t0 instanceof String)) {
              throw new IOException(
                     "Expected a String, but this is a "
                             + t0.getClass().getName());
             }


            //TODO: test this
            stream = analyzer.reusableTokenStream(field, new StringReader((String)t0));

            try {
                while (stream.incrementToken()) {
                    String token = stream.getAttribute(TermAttribute.class).term();
                    out.add(tupleFactory.newTuple(token));
                }
            }
            catch (IOException e) {
                   throw e;
            }
            return out;

    }

    public List<String> getCacheFiles() {
        List<String> list = new ArrayList<String>(1);

        list.add(stoplist);
        return list;
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






