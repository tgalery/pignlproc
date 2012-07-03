package pignlproc.helpers;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import java.io.IOException;

/**
 /**
 * @author chrishokamp
 * Take a bag of paragraphs and emit a single tuple with a single field
 *
 */

public class Concatenate extends EvalFunc<String> {
    @Override
    public String exec (Tuple input) throws IOException {

        Object t0 = input.get(0);
        if (!(t0 instanceof DataBag)) {
            throw new IOException(
                    "Expected bag of paragraphs, but this is a "
                            + t0.getClass().getName());

        }
        DataBag allParagraphs = (DataBag) t0;

        String buffer = "";

        for (Tuple t : allParagraphs)
        {
            buffer += " " + (String)t.get(0);
        }
        return buffer;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }
}

