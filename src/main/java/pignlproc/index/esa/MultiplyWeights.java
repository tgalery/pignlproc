package pignlproc.index.esa;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Chris Hokamp
 */
public class MultiplyWeights extends EvalFunc<DataBag> {
    TupleFactory tupleFactory = TupleFactory.getInstance();
    BagFactory bagFactory = BagFactory.getInstance();

    public DataBag exec(Tuple input) throws IOException {
        //first get the weight for the token
        // then get the bag, and iterate over it, multiplying token values

        DataBag out =  bagFactory.newDefaultBag();
        Object t0 = input.get(0);

        if (!(t0 instanceof Double)) {
            throw new IOException(
                    "Expected a Double, but this is a "
                            + t0.getClass().getName());
        }


        Double tokenWeight = (Double)t0;

        Object t1 = input.get(1);
        if (!(t1 instanceof DataBag)) {
            throw new IOException("Expected input to be bag of (uri, weight) tuples, but got "
                    + t1.getClass().getName());
        }
        DataBag resourceVec = (DataBag)t1;

        for (Tuple t : resourceVec) {
            String uri = (String)t.get(0);
            Double w = (Double)t.get(1);
            //Scale the weight by the token's weight for this resource
            Double scaledWeight = w * tokenWeight;

            out.add(tupleFactory.newTupleNoCopy(Arrays.asList(uri, scaledWeight)));

        }
        return out;
    }

    public Schema outputSchema(Schema input) {
        try {
            Schema tupleSchema = new Schema();
            tupleSchema.add(new FieldSchema("token", DataType.CHARARRAY));
            tupleSchema.add(new FieldSchema("weight", DataType.DOUBLE));
            return new Schema(new Schema.FieldSchema(getSchemaName(
                    this.getClass().getName().toLowerCase(), input),
                    tupleSchema, DataType.BAG));
        } catch (Exception e) {
            return null;
        }
    }

}
