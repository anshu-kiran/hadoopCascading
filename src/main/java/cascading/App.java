package cascading;

import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.util.Properties;

public class App {
    public static void main(String[] args) {
        String inPath = args[0];
        String outPath = args[1];

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, App.class);
        AppProps.setApplicationName(properties, "Hadoop using Cascading");

        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

        Tap inTap = new Hfs(new TextDelimited(true, "|"), inPath);

        Tap outTap = new Hfs(new TextDelimited(true, "|"), outPath);

        Pipe pipe = new Pipe("MapReduce");
        pipe = new GroupBy(pipe, new Fields("Part of the day"));
        pipe = new Every(pipe, new Fields("Part of the day"), new Count(new Fields("count")));

        FlowDef flowDef = FlowDef.flowDef().addSource(pipe, inTap).addTailSink(pipe, outTap).setName("MapReduce");

        flowConnector.connect(flowDef).complete();
    }
}
