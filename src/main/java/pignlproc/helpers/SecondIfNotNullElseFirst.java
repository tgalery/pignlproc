package pignlproc.helpers;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Can be used to resolve redirects
 * or to establish default values for nulls.
 *
 * Takes a 2-element tuple of strings.
 * Checks the 2nd string.
 * If it is null, returns the 1st string.
 * If it is not,  returns the 2nd string.
 */
public class SecondIfNotNullElseFirst extends EvalFunc<Tuple> {

    TupleFactory tupleFactory = TupleFactory.getInstance();

    /*
     * Takes a 2-element tuple of strings.
     * Checks the 2nd string.
     * If it is null, returns the 1st string.
     * If it is not,  returns the 2nd string.
      */
    @Override
    public Tuple exec(Tuple input) throws IOException {
        Object second = input.get(1);
        if (second == null) {
            return tupleFactory.newTuple(input.get(0));
        }
        else {
            return tupleFactory.newTuple(second);
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            Schema tupleSchema = new Schema();
            tupleSchema.add(new Schema.FieldSchema("secondIfNotNullElseFirst", DataType.CHARARRAY));
            return new Schema(new Schema.FieldSchema(getSchemaName(
                    this.getClass().getName().toLowerCase(), input),
                    tupleSchema, DataType.TUPLE));
        } catch (Exception e) {
            return null;
        }
    }

}