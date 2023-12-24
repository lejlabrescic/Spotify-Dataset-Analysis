import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top10MapReduce {

    public static class SongMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable popularity = new IntWritable();
        private Text artistAndSongKey = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (key.get() > 0) {
                String[] fields = value.toString().split(",");
                if (fields.length >= 4) {
                    String artist = fields[1];
                    String song = fields[2];
                    String popularityStr = fields[4];
                    try {
                        int songPopularity = Integer.parseInt(popularityStr);
                        artistAndSongKey.set(artist + " - " + song);
                        popularity.set(songPopularity);
                        context.write(artistAndSongKey, popularity);
                    } catch (NumberFormatException e) {
                        printStackTrace();
                    }
                }
            }
        }
    }

    public static class PopularityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<Integer, Text> popularityMap;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            popularityMap = new TreeMap<>(Comparator.reverseOrder());
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int popularitySum = 0;
            for (IntWritable val : values) {
                popularitySum += val.get();
            }
            popularityMap.put(popularitySum, new Text(key));

            if (popularityMap.size() > 10) {
                popularityMap.pollLastEntry();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, Text> entry : popularityMap.entrySet()) {
                context.write(entry.getValue(), new IntWritable(entry.getKey()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 10 Songs by Popularity");
        job.setJarByClass(Top10MapReduce.class);
        job.setMapperClass(SongMapper.class);
        job.setReducerClass(PopularityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
