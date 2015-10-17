/**
 * Created by gokul on 10/12/15.
 */
import com.sun.jersey.core.util.KeyComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SecondarySort;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class PageRank {

    public static void main(String[] args) throws IOException {
        String input = args[0];
        String output = args[1];

        String outlink = "PageRank.outlink.out";
        String count = "PageRank.n.out";

        String tempDirJob1 = output.concat("/tmp/Job1/");
        parseXML(input, tempDirJob1);
        String tempDirJob2 = output.concat("/tmp/Job2/");
        getAdjacencyGraph(tempDirJob1, tempDirJob2);
        String tempDirJob3 = output.concat("/tmp/Job3/");
        calTotalPages(tempDirJob2, tempDirJob3);

        organiseOutputFiles(tempDirJob2, output + "/results/" + outlink);
        organiseOutputFiles(tempDirJob3, output + "/results/" + count);
        long numberOfPages = 0;
        numberOfPages = getNumberOfPages(output + "/results/" + count);

        int noOfIterations = 8;
        String[] iterations = new String[noOfIterations+1];
        for ( int i =0; i <= noOfIterations ; i++){
            iterations[i] = "PageRank.iter" + Integer.toString(i) +".out";
        }

        String tempDirJob4 = output.concat("/tmp/Job4/");
        initializePageRank(output + "/results/" + outlink, tempDirJob4 + iterations[0], numberOfPages);
        for ( int i = 1; i <= noOfIterations; i++ ) {
            calculatePageRankOnIteration(tempDirJob4+iterations[i-1], tempDirJob4+iterations[i], numberOfPages);
        }

        String tempDirSort = output.concat("/tmp/Sort/");
        sortPagesByRank(tempDirJob4+iterations[1],tempDirSort+iterations[1]+".sort", numberOfPages);
        sortPagesByRank(tempDirJob4+iterations[8],tempDirSort+iterations[8]+".sort", numberOfPages);

        organiseOutputFiles(tempDirSort+iterations[1]+".sort", output+"/results/"+"PageRank.iter1.out");
        organiseOutputFiles(tempDirSort+iterations[8]+".sort", output+"/results/"+"PageRank.iter8.out");

    }

    private static void sortPagesByRank(String inputDir, String outputDir, long numberOfPages) throws IOException {
        JobConf sortJob = new JobConf(PageRank.class);
        sortJob.setJarByClass(PageRank.class);

        FileInputFormat.setInputPaths(sortJob, new Path(inputDir));
        sortJob.setMapperClass(SortPageByRankMapper.class);

        FileOutputFormat.setOutputPath(sortJob, new Path(outputDir));
        sortJob.setReducerClass(SortPageByRankReducer.class);

        sortJob.setMapOutputKeyClass(DoubleWritable.class);
        sortJob.setMapOutputValueClass(Text.class);

        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(DoubleWritable.class);

        sortJob.setOutputKeyComparatorClass(MyKeyComparison.class);
        sortJob.setOutputValueGroupingComparator(MyGroupComparison.class);
        sortJob.setPartitionerClass(MyPartitioner.class);

        sortJob.set("numberOfPages", Long.toString(numberOfPages));

        JobClient.runJob(sortJob);
    }

    private static void initializePageRank(String inputDir, String outputDir, long numberOfPages) throws IOException {
        JobConf rankInitJob = new JobConf(PageRank.class);
        rankInitJob.setJarByClass(PageRank.class);

        FileInputFormat.setInputPaths(rankInitJob, new Path(inputDir));
        rankInitJob.setMapperClass(RankInitMapper.class);

        FileOutputFormat.setOutputPath(rankInitJob, new Path(outputDir));
        rankInitJob.setReducerClass(RankInitReducer.class);

        rankInitJob.setOutputKeyClass(Text.class);
        rankInitJob.setOutputValueClass(Text.class);

        rankInitJob.set("numberOfPages", Long.toString(numberOfPages));

        JobClient.runJob(rankInitJob);
    }

    private static void calculatePageRankOnIteration(String inputDir, String outputDir, long numberOfPages) throws IOException {
        JobConf rankInitJob = new JobConf(PageRank.class);
        rankInitJob.setJarByClass(PageRank.class);

        FileInputFormat.setInputPaths(rankInitJob, new Path(inputDir));
        rankInitJob.setMapperClass(RankMapper.class);

        FileOutputFormat.setOutputPath(rankInitJob, new Path(outputDir));
        rankInitJob.setReducerClass(RankReducer.class);

        rankInitJob.setOutputKeyClass(Text.class);
        rankInitJob.setOutputValueClass(Text.class);

        rankInitJob.set("numberOfPages", Long.toString(numberOfPages));

        JobClient.runJob(rankInitJob);
    }

    private static long getNumberOfPages(String fullFileName) {

        long numberOfPages = 1; //Initialize to 1 as it will be in denominator here after
        BufferedReader reader = null;
        FileSystem fs = null;
        try {
            numberOfPages = 1;
            Path path = new Path(fullFileName);
            fs = path.getFileSystem(new Configuration());
            reader = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line = reader.readLine();

            if (line != null && !line.isEmpty() && line.length() > 2) {
                String[] parts = line.split("=");
                numberOfPages = Integer.parseInt(parts[1]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return (numberOfPages);

    }

    private static void organiseOutputFiles(String input, String output) throws IOException {
        String outputFiles = input + "/part-r-00";
        NumberFormat nf = new DecimalFormat("000");

        Configuration conf = new Configuration();
        FileSystem outFS = null;

        Path outFile = new Path(output);
        outFS = outFile.getFileSystem(new Configuration());
        if (outFS.exists(outFile)) {
            System.out.println("Output File already exists");
            System.exit(1);
        }

        FSDataOutputStream out = outFS.create(outFile);

        Path inFile = new Path (outputFiles + nf.format(0));
        FileSystem inFS = inFile.getFileSystem(new Configuration());

        if (!inFS.exists(inFile)){
            outputFiles = input + "/part-00";
        }

        int counter = 0;
        inFile = new Path(outputFiles+nf.format(counter));
        inFS = inFile.getFileSystem(new Configuration());
        do {
            int bytesRead = 0;
            byte[] buffer = new byte[4096];
            FSDataInputStream in = inFS.open(inFile);
            while((bytesRead = in.read(buffer)) > 0)
                out.write(buffer, 0, bytesRead);
            in.close();
            inFS.close();
            counter++;
            inFile = new Path(outputFiles+nf.format(counter));
            inFS = inFile.getFileSystem(new Configuration());
        } while (inFS.isFile(inFile));

        out.close();

    }

    private static void calTotalPages(String input, String output) throws IOException {
        JobConf numPageJob = new JobConf(PageRank.class);
        numPageJob.setJarByClass(PageRank.class);

        //Configure the inlink generation mapper
        FileInputFormat.setInputPaths(numPageJob, new Path(input));
        numPageJob.setInputFormat(TextInputFormat.class);
        numPageJob.setMapOutputKeyClass(LongWritable.class);
        numPageJob.setMapOutputValueClass(IntWritable.class);
        numPageJob.setMapperClass(CountPagesMapper.class);

        //Configure the inlink genration reducer
        FileOutputFormat.setOutputPath(numPageJob, new Path(output));
        numPageJob.setReducerClass(CountPagesReducer.class);

        //Start the hadoop job
        JobClient.runJob(numPageJob);
    }

    private static void getAdjacencyGraph(String input, String output) throws IOException {
        JobConf adjCreJob = new JobConf(PageRank.class);
        adjCreJob.setJarByClass(PageRank.class);

        //Configure the inlink generation mapper
        FileInputFormat.setInputPaths(adjCreJob, new Path(input));
        adjCreJob.setInputFormat(TextInputFormat.class);
        adjCreJob.setMapperClass(AdjGraphCreMapper.class);

        //Configure the inlink genration reducer
        FileOutputFormat.setOutputPath(adjCreJob, new Path(output));
        adjCreJob.setReducerClass(AdjGraphCreReducer.class);

        //Define the output key and value classes
        adjCreJob.setOutputKeyClass(Text.class);
        adjCreJob.setOutputValueClass(Text.class);

        //Start the hadoop job
        JobClient.runJob(adjCreJob);
    }

    public static void parseXML(String input, String output) throws IOException {
        JobConf redLinkRemovalJob = new JobConf(PageRank.class);

        System.out.println("Input: "+input+"\nOutput: "+output);
        redLinkRemovalJob.set(XmlInputFormat.START_TAG_KEY, "<page>");
        redLinkRemovalJob.set(XmlInputFormat.END_TAG_KEY, "</page>");
        redLinkRemovalJob.setJarByClass(PageRank.class);

        //Configure the inlink generation mapper
        FileInputFormat.setInputPaths(redLinkRemovalJob, new Path(input));
        redLinkRemovalJob.setInputFormat(XmlInputFormat.class);
        redLinkRemovalJob.setMapperClass(RedLinkMapper.class);

        //Configure the inlink genration reducer
        FileOutputFormat.setOutputPath(redLinkRemovalJob, new Path(output));
        redLinkRemovalJob.setReducerClass(RedLinkReducer.class);

        //Defince the output key and value classes
        redLinkRemovalJob.setOutputKeyClass(Text.class);
        redLinkRemovalJob.setOutputValueClass(Text.class);

        //Start the hadoop job
        JobClient.runJob(redLinkRemovalJob);
    }
}
