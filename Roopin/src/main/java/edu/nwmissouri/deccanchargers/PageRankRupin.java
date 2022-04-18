package edu.nwmissouri.deccanchargers;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;

public class PageRankRupin {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pl = Pipeline.create(options);
        String dataFolder = "web04";

        PCollection<KV<String, String>> pColctn1 = RupinMapper01(pl, "java.md", dataFolder);
        PCollection<KV<String, String>> pColctn2 = RupinMapper01(pl, "go.md", dataFolder);
        PCollection<KV<String, String>> pColctn3 = RupinMapper01(pl, "python.md", dataFolder);
        PCollection<KV<String, String>> pColctn4 = RupinMapper01(pl, "README.md", dataFolder);

        PCollectionList<KV<String, String>> pColctnList = PCollectionList.of(pColctn1).and(pColctn2).and(pColctn3)
                .and(pColctn4);
        PCollection<KV<String, String>> merList = pColctnList.apply(Flatten.<KV<String, String>>pCollections());
        PCollection<String> merListStr = merList
                .apply(MapElements.into(TypeDescriptors.strings()).via((mergeOut) -> mergeOut.toString()));
        merListStr.apply(TextIO.write().to("RupinPR"));
        pl.run().waitUntilFinish();
    }

    public static PCollection<KV<String, String>> RupinMapper01(Pipeline p, String filename, String dataFolder) {
        String dataPath = dataFolder + "/" + filename;
        PCollection<String> pColctnInp = p.apply(TextIO.read().from(dataPath));
        PCollection<String> pColctnLinkLines = pColctnInp.apply(Filter.by((String line) -> line.startsWith("[")));
        PCollection<String> pColctnLinks = pColctnLinkLines.apply(MapElements.into((TypeDescriptors.strings()))
                .via((String linkLine) -> linkLine.substring(linkLine.indexOf("(") + 1, linkLine.length() - 1)));
        PCollection<KV<String, String>> pColctnKVPairs = pColctnLinks
                .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via((String outputLink) -> KV.of(filename, outputLink)));
        return pColctnKVPairs;
    }

}