package pignlproc.helpers;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Chris Hokamp
 */
public class FirstNtuples extends EvalFunc<DataBag> {
    BagFactory bagFactory = BagFactory.getInstance();
    Integer numberToKeep;

    public FirstNtuples(String n) {
        try {
            numberToKeep = Integer.parseInt(n);
        }
        catch (NumberFormatException e) {
            String msg = "error in FirstNtuples - param cannot be converted to Integer";
            throw new NumberFormatException(msg);
        }
    }


    @Override
    public DataBag exec (Tuple input) throws IOException{
        Object t0 = input.get(0);
        if (!(t0 instanceof DataBag)) {
            throw new IOException(
                    "Expected bag of paragraphs, but this is a "
                        + t0.getClass().getName());
        }
        DataBag allParagraphs = (DataBag) t0;
        DataBag out = bagFactory.newDefaultBag();

        if (allParagraphs.size() >= numberToKeep) {
            Iterator<Tuple> it = allParagraphs.iterator();
            for (int i = 0; i < numberToKeep; i++) {
                out.add(it.next());
            }
        } else {
            out.addAll(allParagraphs);
        }
        return out;
    }

    public Schema outputSchema(Schema input) {
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
