package pignlproc.evaluation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import opennlp.tools.util.Span;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;


/**
 * @author chrishokamp
 * Parse the text - for each link emit a tuple with the link and the paragraph in which it occurred
 *
 *
 */
public class ParagraphsWithLink extends EvalFunc<DataBag> {

    TupleFactory tupleFactory = TupleFactory.getInstance();

    BagFactory bagFactory = BagFactory.getInstance();

    Integer maxSpanLength;


    public ParagraphsWithLink(String max) throws IOException {
        try {
            maxSpanLength = Integer.parseInt(max);
        }
        catch (NumberFormatException e)
        {
            String msg = "error in ParagraphsWithLink - param cannot be converted to Integer";
            throw new NumberFormatException(msg);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataBag exec(Tuple input) throws IOException {

        DataBag output = bagFactory.newDefaultBag();
        Object t0 = input.get(0);
        if (!(t0 instanceof String)) {
            throw new IOException("Expected input to be chararray, but got "
                    + t0.getClass().getName());
        }
        Object t1 = input.get(1);
        if (!(t1 instanceof DataBag)) {
            throw new IOException("Expected input to be bag of links, but got "
                    + t1.getClass().getName());
        }
        Object t2 = input.get(2);
        //Chris: changed t1 to t2 in error checking below (this was an error)
        if (!(t2 instanceof DataBag)) {
            throw new IOException(
                    "Expected input to be bag of paragraphs, but got "
                            + t2.getClass().getName());
        }
        String text = (String) t0;
        DataBag links = (DataBag) t1;
        DataBag paragraphBag = (DataBag) t2;

        // convert the bag of links as absolute spans over the text
        List<Span> linkSpans = new ArrayList<Span>();
        for (Tuple l : links) {
            linkSpans.add(new Span((Integer) l.get(1), (Integer) l.get(2),
                    (String) l.get(0)));
        }
        Collections.sort(linkSpans);

        //iterate over paragraphs and return one tuple for each link in that paragraph
        int order = 0; //this is the paragraph index
        for (Tuple p : paragraphBag) {
            order++;
            Integer beginParagraph = (Integer) p.get(1);
            Integer endParagraph = (Integer) p.get(2);
            String paragraph = text.substring(beginParagraph,
                    endParagraph);
            // replace some formatting white-spaces without changing the
            // number of chars not to break the annotations
            paragraph = paragraph.replaceAll("\n", " ");
            paragraph = paragraph.replaceAll("\t", " ");
            if (paragraph.length() < maxSpanLength)
            {
                //Create a Span so that we can use contains(Span link)
                Span paragraph_span = new Span(beginParagraph, endParagraph);
                //now iterate over the list of links until the next link is no longer contained in this paragraph - once the tuple is created, remove the link from the list to leverage the ordering
                for (Span link : linkSpans) {
                    if (paragraph_span.contains(link)) {
                        int begin = link.getStart()
                                - paragraph_span.getStart();
                        int end = link.getEnd() - paragraph_span.getStart();
                        output.add(tupleFactory.newTupleNoCopy(Arrays.asList(
                                order, paragraph, link.getType(), begin, end)));
                        //linkSpans.remove(link);

                    }
                    else if (link.compareTo(paragraph_span) > 1) {
                        break;
                    }
                  }
              }
          }

          return output;

    }

    public Schema outputSchema(Schema input) {
        try {
            Schema tupleSchema = new Schema();
            tupleSchema.add(new FieldSchema("paragraphOrder", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("paragraph", DataType.CHARARRAY));
            tupleSchema.add(new FieldSchema("linkTarget", DataType.CHARARRAY));
            tupleSchema.add(new FieldSchema("linkBegin", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("linkEnd", DataType.INTEGER));
            return new Schema(new Schema.FieldSchema(getSchemaName(
                    this.getClass().getName().toLowerCase(), input),
                    tupleSchema, DataType.BAG));
        } catch (Exception e) {
            return null;
        }
    }






}
