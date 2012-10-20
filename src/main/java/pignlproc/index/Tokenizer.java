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
 * Take a bag of paragraphs (usage contexts) and emit a bag of tokens using the OpenNLP tokenizer
 *
 */

public class Tokenizer extends EvalFunc<DataBag> {

    public static final String ENGLISH_TOKENMODEL_PATH = "opennlp/en-token.bin";

    TupleFactory tupleFactory = TupleFactory.getInstance();

    BagFactory bagFactory = BagFactory.getInstance();

    protected final TokenizerModel model;

    public Tokenizer() throws IOException {
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

    @Override
    public DataBag exec(Tuple input) throws IOException {
        TokenizerME tokenizer = new TokenizerME(model);
        DataBag out = bagFactory.newDefaultBag();
        Object t0 = input.get(0);
        if (!(t0 instanceof String)) {
            throw new IOException(
                    "Expected a String, but this is a "
                            + t0.getClass().getName());

        }

        // Update 5.6.12: get counts in pig, here, just output one tuple for each token
        // think about how to modularize the preprocessing


        //there should be only one item in each tuple
        String[] tokens = tokenizer.tokenize((String)t0);
        for (String token : tokens)
        {
            out.add(tupleFactory.newTuple(token));
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

