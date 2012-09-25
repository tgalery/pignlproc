package pignlproc.index.esa;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Chris Hokamp
 *
 * Take a bag of bags of URIs as input, and create a HashMap to hold the URIs as keys
 *  - iterate over each bag and sum the URIs inside the hash
 *  - Finally, get the centroid, and return a single bag
 *  - this may cause memory problems for very big bags
 *
 */
public class AggregateAndAverage extends EvalFunc<DataBag> {
    TupleFactory tupleFactory = TupleFactory.getInstance();
    BagFactory bagFactory = BagFactory.getInstance();
    HashMap<String,Double> resourceVector = new HashMap<String, Double>();

    @Override
    public DataBag exec(Tuple input) throws IOException {

        DataBag out = bagFactory.newDefaultBag();

        Object t0 = input.get(0);
        if (!(t0 instanceof DataBag)) {
            throw new IOException("Expected input to be bag of (uri, weight) tuples, but got "
                    + t0.getClass().getName());
        }
        //this is a bag of bags --> the nested loops introduce exponential complexity(!)
        DataBag resourceVec = (DataBag)t0;
        long numTokens = resourceVec.size();
        for (Tuple t : resourceVec) {
            DataBag tokensResources = (DataBag)t.get(0);
            for (Tuple rw : tokensResources) {
                String resUri = (String)rw.get(0);
                Double resWeight = (Double)rw.get(1);
                if (resourceVector.containsKey(resUri)) {
                    Double val = resourceVector.get(resUri) + resWeight;
                    resourceVector.put(resUri, val);
                } else {
                   resourceVector.put(resUri, resWeight);
                }


            }
        }
        //Now get the centroid, create a new tuple, and add to the bag
        Set<Map.Entry<String,Double>> entrySet = resourceVector.entrySet();
        for (Map.Entry e : entrySet) {
            String res = (String)e.getKey();
            Double avg = (Double)e.getValue()/numTokens;

            out.add(tupleFactory.newTupleNoCopy(Arrays.asList(res, avg)));

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
