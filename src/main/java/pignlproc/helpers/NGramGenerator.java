package pignlproc.helpers;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This class inspired by a class from the Pig Tutorial:
 * https://cwiki.apache.org/confluence/display/PIG/PigTutorial
 */

import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.util.Span;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * This function divides a search query string into words and extracts
 * n-grams with up to NGRAM_SIZE_LIMIT length.
 * Example 1: if query = "a real nice query" and NGRAM_SIZE_LIMIT = 2,
 * the query is split into: a, real, nice, query, a real, real nice, nice query
 * Example 2: if record = (u1, h1, pig hadoop) and NGRAM_SIZE_LIMIT = 2,
 * the record is split into: (u1, h1, pig), (u1, h1, hadoop), (u1, h1, pig hadoop)
 */
public class NGramGenerator extends EvalFunc<DataBag> {

    private int ngramSizeLimit;

    private final Tokenizer tokenizer = SimpleTokenizer.INSTANCE;

    private final BagFactory bagFactory = DefaultBagFactory.getInstance();
    private final TupleFactory tupleFactory = TupleFactory.getInstance();

    public NGramGenerator(int ngramSizeLimit) {
        this.ngramSizeLimit = ngramSizeLimit;
    }

    // Pig versions < 0.9 seem to only pass strings in constructor
    public NGramGenerator(String ngramSizeLimit) {
        this(Integer.valueOf(ngramSizeLimit));
    }

    @Override
    public DataBag exec(Tuple input) throws IOException {
        String text = (String)input.get(0);
        Span[] spans = tokenizer.tokenizePos(text);
        DataBag output = bagFactory.newDefaultBag();
        fillOutputWithNgrams(spans, text, output, this.ngramSizeLimit);
        return output;
    }

    /**
     * This is a simple utility function that make word-level ngrams from a set of words.
     * @param spans spans of tokens in the text
     * @param text the original text
     * @param output result bag of ngram strings
     * @param size number of words in an ngram (in this recursive call)
     */
    private void fillOutputWithNgrams(Span[] spans, String text, DataBag output, int size) {
        int stop = spans.length - size + 1;
        for (int startIdx = 0; startIdx < stop; startIdx++) {
            int endIdx = startIdx + size - 1;
            String ngram = text.substring(spans[startIdx].getStart(), spans[endIdx].getEnd());
            Tuple tuple = tupleFactory.newTuple(ngram);
            output.add(tuple);
        }
        if (size > 1) {
            fillOutputWithNgrams(spans, text, output, size - 1);
        }
    }

    /**
     * This method gives a name to the column.
     * @param input - schema of the input data
     * @return schema of the input data
     */
    @Override
    public Schema outputSchema(Schema input) {
         Schema bagSchema = new Schema();
         bagSchema.add(new Schema.FieldSchema("ngram", DataType.CHARARRAY));
         try {
            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                                                    bagSchema, DataType.BAG));
         } catch (FrontendException e) {
            return null;
         }
    }

    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
     * This is needed to make sure that both bytearrays and chararrays can be passed as arguments
     */
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>(1);
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));
        return funcList;
    }

}