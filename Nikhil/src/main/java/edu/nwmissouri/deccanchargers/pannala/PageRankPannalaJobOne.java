package edu.nwmissouri.deccanchargers.pannala;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;

public class PageRankPannalaJobOne {
  // DEFINE DOFNS
  // ==================================================================
  // You can make your pipeline assembly code less verbose by defining
  // your DoFns statically out-of-line.
  // Each DoFn<InputT, OutputT> takes previous output
  // as input of type InputT
  // and transforms it to OutputT.
  // We pass this DoFn to a ParDo in our pipeline.

  /**
   * DoFn Job1Finalizer takes KV(String, String List of outlinks) and transforms
   * the value into our custom RankedPage Value holding the page's rank and list
   * of voters.
   * 
   * The output of the Job1 Finalizer creates the initial input into our
   * iterative Job 2.
   */
  static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection<String>) element.getValue()).size();
      }
      ArrayList<VotingPage> voters = new ArrayList<VotingPage>();
      for (String voterName : element.getValue()) {
        if (!voterName.isEmpty()) {
          voters.add(new VotingPage(voterName, contributorVotes));
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPage(element.getKey(), voters)));
    }
  }

  static class Job2Mapper extends DoFn<KV<String, RankedPage>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, RankedPage> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      int votes = 0;
      ArrayList<VotingPage> voters = element.getValue().getVoterList();
      if (voters instanceof Collection) {
        votes = ((Collection<VotingPage>) voters).size();
      }
      for (VotingPage vp : voters) {
        String pageName = vp.getVoterName();
        double pageRank = vp.getPageRank();
        String contributingPageName = element.getKey();
        double contributingPageRank = element.getValue().getRank();
        VotingPage contributor = new VotingPage(contributingPageName, votes, contributingPageRank);
        ArrayList<VotingPage> arr = new ArrayList<>();
        arr.add(contributor);
        receiver.output(KV.of(vp.getVoterName(), new RankedPage(pageName, pageRank, arr)));
      }
    }
  }

  static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<RankedPage>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Double dampingFactor = 0.85;
      Double updatedRank = (1 - dampingFactor);
      ArrayList<VotingPage> newVoters = new ArrayList<>();
      for (RankedPage rankPage : element.getValue()) {
        if (rankPage != null) {
          for (VotingPage votingPage : rankPage.getVoterList()) {
            newVoters.add(votingPage);
            updatedRank += (dampingFactor) * votingPage.getPageRank() / (double) votingPage.getContributorVotes();
          }
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPage(element.getKey(), updatedRank, newVoters)));

    }

  }

  static class Job3 extends DoFn<KV<String, RankedPage>, KV<Double, String>> {
    @ProcessElement
    public void processElement(@Element KV<String, RankedPage> element,
        OutputReceiver<KV<Double, String>> receiver) {
      receiver.output(KV.of(element.getValue().getRank(), element.getKey()));
    }
  }

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pl = Pipeline.create(options);
    String dataFolder = "web04";

    PCollection<KV<String, String>> pColctn1 = PannalaMapper01(pl, "java.md", dataFolder);
    PCollection<KV<String, String>> pColctn2 = PannalaMapper01(pl, "go.md", dataFolder);
    PCollection<KV<String, String>> pColctn3 = PannalaMapper01(pl, "python.md", dataFolder);
    PCollection<KV<String, String>> pColctn4 = PannalaMapper01(pl, "README.md", dataFolder);

    PCollectionList<KV<String, String>> pColctnList = PCollectionList.of(pColctn1).and(pColctn2).and(pColctn3)
        .and(pColctn4);
    PCollection<KV<String, String>> merList = pColctnList.apply(Flatten.<KV<String, String>>pCollections());
    PCollection<KV<String, Iterable<String>>> pColGroupByKey = merList.apply(GroupByKey.create());
    PCollection<KV<String, RankedPage>> job2in = pColGroupByKey.apply(ParDo.of(new Job1Finalizer()));

    PCollection<KV<String, RankedPage>> job2out = null;

    int numOfIterations = 40;
    for (int i = 1; i <= numOfIterations; i++) {
      PCollection<KV<String, RankedPage>> job2Mapper = job2in.apply(ParDo.of(new Job2Mapper()));

      PCollection<KV<String, Iterable<RankedPage>>> job2MapperGrpByKey = job2Mapper.apply(GroupByKey.create());

      job2out = job2MapperGrpByKey.apply(ParDo.of(new Job2Updater()));
      job2in = job2out;
    }

    PCollection<String> pColctnStringLists = job2out.apply(
        MapElements.into(
            TypeDescriptors.strings()).via(
                kvtoString -> kvtoString.toString()));
    pColctnStringLists.apply(TextIO.write().to("PannalaPR"));

    pl.run().waitUntilFinish();
  }

  public static PCollection<KV<String, String>> PannalaMapper01(Pipeline p, String filename, String dataFolder) {
    PCollection<String> pColInputLines = p.apply(TextIO.read().from(dataFolder + "/" + filename));
    PCollection<String> pColLinkLines = pColInputLines.apply(Filter.by((String line) -> line.startsWith("[")));
    PCollection<String> pColLinks = pColLinkLines.apply(
        MapElements.into(
            TypeDescriptors.strings())
            .via(
                (String linkLine) -> linkLine.substring(linkLine.indexOf("(") + 1, linkLine.indexOf(")"))));

    PCollection<KV<String, String>> pColkvs = pColLinks.apply(
        MapElements.into(
            TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings())).via(
                outLink -> KV.of(filename, outLink)));
    return pColkvs;
  }

}