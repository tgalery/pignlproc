package pignlproc.index;

import java.io.IOException;
import java.util.*;
import java.io.InputStream;

import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;

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

public class GetCounts extends EvalFunc<DataBag> {

    public static final String ENGLISH_TOKENMODEL_PATH = "opennlp/en-token.bin";

    TupleFactory tupleFactory = TupleFactory.getInstance();

    BagFactory bagFactory = BagFactory.getInstance();

    protected final TokenizerModel model;

    public GetCounts() throws IOException {
        ClassLoader loader = getClass().getClassLoader();

        String path = ENGLISH_TOKENMODEL_PATH;
        InputStream in = loader.getResourceAsStream(path);
        if (in == null) {
            String message = String.format("Couldn't find resource for model" + "tokenizer model: %s", path);
            //log.error(message);
            throw new IOException(message);
        }
        model = new TokenizerModel(in);
    }

    //get the input bag
    // take each tuple separately

    @Override
    public DataBag exec(Tuple input) throws IOException {
        TokenizerME tokenizer = new TokenizerME(model);
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
        // TODO: think about a more efficient way to do this
        // Update 5.6.12: get counts in pig, here, just output one tuple for each token
        // think about how to modularize the preprocessing

        while (it.hasNext())
        {
            //there should be only one item in each tuple
            String text = (String) it.next().get(0);
            String[] tokens = tokenizer.tokenize(text);

            for (String token : tokens)
            {
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

