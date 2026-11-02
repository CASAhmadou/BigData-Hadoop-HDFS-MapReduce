package hadoop.mapreduce.tp1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Locale;

/**
 * Reducer : additionne tous les montants d'un magasin
 * et écrit (magasin, total) avec 2 décimales.
 */
public class SalesReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    private final Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
        }
        // Locale.US -> point décimal ; évite aussi la notation 1.07E7
        result.set(String.format(Locale.US, "%.2f", sum));
        context.write(key, result);
    }
}