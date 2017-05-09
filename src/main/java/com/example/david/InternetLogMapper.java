package com.example.david;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * Created by David on 5/9/17.
 */
public class InternetLogMapper extends Mapper<Object, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    //test
    private static int test = 1;

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //test
        if (test == 1) {
            System.out.println("This is Mapper!");
            test++;
        }

        String inputLine = value.toString();
        String[] inputLineArray = inputLine.split("\t");

        if (inputLineArray.length == 11) {
            outputKey.set(inputLineArray[1]);
            String result = inputLineArray[6] + " " + inputLineArray[7] + " " + inputLineArray[8] + " " + inputLineArray[9];
            outputValue.set(result);

            context.write(outputKey, outputValue);
        }

    }

}
