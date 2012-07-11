package pignlproc.index;

import java.io.*;
import java.util.*;

import com.bea.xml.stream.StaticAllocator;

import com.google.common.io.Files;
import com.google.common.collect.Sets;
import com.google.common.base.Charsets;

import org.apache.commons.lang.CharSet;
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


/**
 /**
 * @author chrishokamp
 * Take a bag of paragraphs (usage contexts) and emit a bag of tuples with (word, count)
 *
 */

public class GetCountsLucene extends EvalFunc<DataBag> {

    TupleFactory tupleFactory = TupleFactory.getInstance();
    BagFactory bagFactory = BagFactory.getInstance();

    //TODO: test
    //String language;

    //Hard-coded for the Lucene standard analyzer because this is unnecessary for this implementation
    static final String field = "paragraph";

    private String stoplist_path; //the path to the stoplist
    private String stoplist_name; //the name of the stoplist
    private HashSet<String> stopset = null;
    protected Analyzer analyzer;
    private TokenStream stream = null;


    public GetCountsLucene (String path, String name) throws IOException {
        stoplist_path = path;
        stoplist_name = name;
        //language = lang;
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

            BufferedReader br = new BufferedReader(fr);
            String line = null;
            stopset = new HashSet<String>();
            while ((line = br.readLine()) != null)
            {
                   stopset.add(line);
            }

            analyzer = new EnglishAnalyzer(Version.LUCENE_36, stopset);
        }

        DataBag out = bagFactory.newDefaultBag();
        Object t0 = input.get(0);
        if (!(t0 instanceof DataBag)) {
            throw new IOException(
                    "Expected bag of paragraphs, but this is a "
                            + t0.getClass().getName());

        }

        DataBag allParagraphs = (DataBag) t0;
        //Iterator<Tuple> it = allParagraphs.iterator();

        Map <String, Integer> allCounts = new HashMap<String, Integer>();
        // TODO: this was necessary due to limited space on cluster


        //TODO: testing here
        for (Tuple t : allParagraphs)
        {
            //there should be only one item in each tuple
            String text = (String)t.get(0);

            stream = analyzer.reusableTokenStream(field, new StringReader(text));

             while (stream.incrementToken()) {
                 String token = stream.getAttribute(CharTermAttribute.class).toString();
                 if (!(allCounts.containsKey(token)))
                 {
                     allCounts.put(token, 1);
                 }
                 else
                 {
                     Integer i = allCounts.get(token)+1;
                     allCounts.put(token, i);
                 }
             }

        }

        //add totals to output
        Iterator tuples = allCounts.entrySet().iterator();
        while (tuples.hasNext())
        {
            Map.Entry t = (Map.Entry)tuples.next();
            String tok = (String)t.getKey();
            Integer total = (Integer)t.getValue();
            out.add(tupleFactory.newTupleNoCopy(Arrays.asList(
                    tok, total)));
        }
        return out;
    }

    public Schema outputSchema(Schema input) {
        try {
            Schema tupleSchema = new Schema();
            tupleSchema.add(new FieldSchema("token", DataType.CHARARRAY));
            tupleSchema.add(new FieldSchema("count", DataType.INTEGER));
            return new Schema(new Schema.FieldSchema(getSchemaName(
                    this.getClass().getName().toLowerCase(), input),
                    tupleSchema, DataType.BAG));
        } catch (Exception e) {
            return null;
        }

    }

}