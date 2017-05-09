package com.example.david;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by David on 5/9/17.
 */
public class InternetLogReducer extends Reducer<Text, Text, Text, Text> {
    private Text outputValue = new Text();

    private List<Integer> upPackNum = new ArrayList<Integer>();
    private List<Integer> downPackNum = new ArrayList<Integer>();
    private List<Integer> upPayLoad = new ArrayList<Integer>();
    private List<Integer> downPayLoad = new ArrayList<Integer>();

    //test
    private static int test = 1;

    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

        int totalOfUpPackNum = 0, totalOfDownPackNum = 0, totalOfUpPayLoad = 0, totalOfDownPayLoad = 0; //define number of total.

        //test
        if (test == 1) {
            System.out.println("This is Reducer!");
            test++;
        }

        for (Text val : value) {
            String valToStr = val.toString();
            String[] valToStrArr = valToStr.split(" ");
            int count = 1;
            for (int i = 0; i < valToStrArr.length; i++) {
                if (count == 1)
                    upPackNum.add(Integer.parseInt(valToStrArr[i]));
                if (count == 2)
                    downPackNum.add(Integer.parseInt(valToStrArr[i]));
                if (count == 3)
                    upPayLoad.add(Integer.parseInt(valToStrArr[i]));
                if (count == 4)
                    downPayLoad.add(Integer.parseInt(valToStrArr[i]));
                count++;
            }
        }

        //Calculate total of upPackNum.
        for (int i = 0; i < upPackNum.size(); i++) {
            totalOfUpPackNum += upPackNum.get(i);
        }
//        for (Integer i : upPackNum) {
//            totalOfUpPackNum += i;
//        }

        //Calculate total of downPackNum.
        for (int i = 0; i < downPackNum.size(); i++) {
            totalOfDownPackNum += downPackNum.get(i);
        }

        //Calculate total of upPayLoad.
        for (int i = 0; i < upPayLoad.size(); i++) {
            totalOfUpPayLoad += upPayLoad.get(i);
        }

        //Calculate total of downPayLoad.
        for (int i = 0; i < downPayLoad.size(); i++) {
            totalOfDownPayLoad += downPayLoad.get(i);
        }

        //Connect String.
        String result = totalOfUpPackNum + " " + totalOfDownPackNum + " " + totalOfUpPayLoad + " " + totalOfDownPayLoad;
        outputValue.set(result);

        //context write.
        context.write(key, outputValue);

    }
}
