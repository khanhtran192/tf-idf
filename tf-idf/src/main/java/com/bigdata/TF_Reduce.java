package com.bigdata;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

//public class TF_Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
//
//
//    @Override
//    public void reduce(Text word, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
//        int sum = 0;
//        int cnt = 0;
//
//        for (IntWritable count : counts) {
//            sum += count.get();
//            cnt++;
//        }
////        double tf = Math.log10(10) + Math.log10(sum);
//        double tf = sum/cnt;
//        context.write(word, new DoubleWritable(tf));
//    }
//
//
//}




public class TF_Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {


    private static final DecimalFormat DF = new DecimalFormat("###.########");

    private Text wordAtDocument = new Text();

    private Text tfidfCounts = new Text();

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {

        int numberOfDocumentsInCorpusWhereKeyAppears = 0;
        Map<String, String> tempFrequencies = new HashMap<String, String>();
        for (Text val : values) {
            String[] documentAndFrequencies = val.toString().split("=");

            if (Integer.parseInt(documentAndFrequencies[1].split("/")[0]) > 0) {
                numberOfDocumentsInCorpusWhereKeyAppears++;
            }
            tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
        }
        for (String document : tempFrequencies.keySet()) {
            String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split("/");

            double tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0])
                    / Double.valueOf(wordFrequenceAndTotalWords[1]));

            context.write(this.wordAtDocument, new DoubleWritable(tf));
        }
    }
}



