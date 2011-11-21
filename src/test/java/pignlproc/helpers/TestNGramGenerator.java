package pignlproc.helpers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.junit.Test;

public class TestNGramGenerator {

    TupleFactory tupleFactory = TupleFactory.getInstance();

    private void test(String testString, Set<String> correctNgrams, int ngramLength) throws IOException {
        NGramGenerator generator = new NGramGenerator(ngramLength);
        Tuple inputTuple = tupleFactory.newTuple(testString);
        DataBag resultBag = generator.exec(inputTuple);
        Iterator<Tuple> it = resultBag.iterator();
        while (it.hasNext()) {
            Tuple tuple = it.next();
            String ngram = (String) tuple.get(0);
            Assert.assertTrue("'" + ngram + "' not in corrects: " + correctNgrams, correctNgrams.contains(ngram));
            correctNgrams.remove(ngram);
        }
        Assert.assertSame(0, correctNgrams.size());
    }

    @Test
    public void easy() throws IOException {
        String testString = "This is an easy example";
        Set<String> correctNgrams = new TreeSet<String>();
        correctNgrams.add("This is an");
        correctNgrams.add("is an easy");
        correctNgrams.add("an easy example");
        correctNgrams.add("This is");
        correctNgrams.add("is an");
        correctNgrams.add("an easy");
        correctNgrams.add("easy example");
        correctNgrams.add("This");
        correctNgrams.add("is");
        correctNgrams.add("an");
        correctNgrams.add("easy");
        correctNgrams.add("example");
        test(testString, correctNgrams, 3);
    }

    //@Test
    public void medium() throws IOException {
        String testString = "That's a little harder.";
        Set<String> correctNgrams = new HashSet<String>();

        test(testString, correctNgrams, 3);
    }

}
