package pignlproc.index.esa;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Chris Hokamp
 */
public class Divide extends EvalFunc<DataBag>{
    TupleFactory tupleFactory = TupleFactory.getInstance();
    BagFactory bagFactory = BagFactory.getInstance();

    @Override
    public DataBag exec(Tuple input) throws IOException {

        DataBag out = bagFactory.newDefaultBag();

        Object t0 = input.get(0);
        if (!(t0 instanceof Long)) {
            throw new IOException("Expected input to be a Long, but got "
                    + t0.getClass().getName());
        }
        long numTokens = (Long)t0;
        Object t1 = input.get(1);
        if (!(t1 instanceof DataBag)) {
            throw new IOException("Expected input to be bag of (uri, weight) tuples, but got "
                    + t1.getClass().getName());
        }
        DataBag resourceVec = (DataBag)t1;

        for (Tuple t : resourceVec) {
            String uri = (String)t.get(0);
            Double w = (Double)t.get(1);
            Double avg = w/numTokens;
            out.add(tupleFactory.newTupleNoCopy(Arrays.asList(uri, avg)));

        }
        return out;
    }

    public Schema outputSchema (Schema input) {
        try {
            Schema tupleSchema = new Schema();
            tupleSchema.add(new Schema.FieldSchema("token", DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema("weight", DataType.DOUBLE));
            return new Schema(new Schema.FieldSchema(getSchemaName(
                    this.getClass().getName().toLowerCase(), input),
                    tupleSchema, DataType.BAG));
        } catch (Exception e) {
            return null;
        }
    }





}
