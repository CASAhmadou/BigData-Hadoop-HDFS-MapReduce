package hadoop.mapreduce.tp1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper : pour chaque ligne de purchases.txt
 *   date \t heure \t magasin \t produit \t cout \t paiement
 * émet la paire (magasin, cout).
 */
public class SalesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private final Text store = new Text();
    private final DoubleWritable cost = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Découpage sur la tabulation (et pas sur les espaces :
        // les noms de magasins comme "San Jose" contiennent des espaces)
        String[] fields = value.toString().split("\t");

        // On ignore les lignes mal formées
        if (fields.length != 6) {
            return;
        }

        try {
            store.set(fields[2].trim());
            cost.set(Double.parseDouble(fields[4].trim()));
            context.write(store, cost);
        } catch (NumberFormatException e) {
            // Montant illisible : ligne ignorée
        }
    }
}