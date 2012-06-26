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

    //commented for testing
    //public static final String ENGLISH_STOPWORDS_PATH = "/stopwords.en.list";

    //commented for testing
    //protected final HashSet<String> stopwords;

    TupleFactory tupleFactory = TupleFactory.getInstance();
    BagFactory bagFactory = BagFactory.getInstance();
    //Hard-coded for the Lucene standard analyzer because this is unnecessary for this implementation
    static final String field = "paragraph";

    final String stoplist;


    public GetCountsLucene (String path) throws IOException {

        stoplist = path;

        //commented for testing
        /*
            ClassLoader loader = getClass().getClassLoader();

           String path = ENGLISH_STOPWORDS_PATH;
            InputStream in = loader.getResourceAsStream(path);
            File file = new File(path);

            stopwords = Sets.newHashSet(Files.readLines(file, Charsets.UTF_8));
          */

        }




    public List<String> getCacheFiles() {
        List<String> list = new ArrayList<String>(1);
        //TODO: (Testing) pass as a param! this will only work on the LIT@UNT Hadoop cluster (or any that has the stoplist in hdfs at "/user/hadoop/stopwords.en.list")
        list.add(stoplist);
        return list;
    }

    @Override
    public DataBag exec(Tuple input) throws IOException {

        //commented for testing
        //String path = ENGLISH_STOPWORDS_PATH;

        //commented for testing
        //FileReader fr = new FileReader(path);

        HashSet<String> stopset = new HashSet<String>();
        //uses hadoop distributed cache (via getCacheFiles)
        FileReader fr = new FileReader("./stopwords.en.list");


        BufferedReader br = new BufferedReader(fr);
        String line = null;
        while ((line = br.readLine()) != null)
        {
               stopset.add(line);
        }

        Analyzer analyzer = new EnglishAnalyzer(Version.LUCENE_36, stopset);

        DataBag out = bagFactory.newDefaultBag();
        Object t0 = input.get(0);
        if (!(t0 instanceof DataBag)) {
            throw new IOException(
                    "Expected bag of paragraphs, but this is a "
                            + t0.getClass().getName());

        }

        DataBag allParagraphs = (DataBag) t0;
        Iterator<Tuple> it = allParagraphs.iterator();

        Map <String, Integer> allCounts = new HashMap<String, Integer>();
        // TODO: think about a more efficient way to do this - this was necessary due to limited space on cluster






        while (it.hasNext())
        {
            //there should be only one item in each tuple
            String text = (String) it.next().get(0);

            TokenStream stream = analyzer.tokenStream(field, new StringReader(text));

            try {
                while (stream.incrementToken()) {
                    String token = stream.getAttribute(TermAttribute.class).term();
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
            catch (IOException e) {
                throw e;
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