/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package edu.nwmissouri.deccancharges.Balajisarvepalli;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;


public class MinimalPageRankBalaji {

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

    Pipeline p = Pipeline.create(options);
    String dataFolder = "web04";
   
   
   PCollection<KV<String,String>> pKVcollection1 = BalajiMapper01(p,"go.md",dataFolder);
   PCollection<KV<String,String>> pKVcollection2 = BalajiMapper01(p,"python.md",dataFolder);
   PCollection<KV<String,String>> pKVcollection3 = BalajiMapper01(p,"java.md",dataFolder);
   PCollection<KV<String,String>> pKVcollection4 = BalajiMapper01(p,"README.md",dataFolder);

   
    PCollectionList<KV<String, String>> pCollectionList = PCollectionList.of(pKVcollection1).and(pKVcollection2).and(pKVcollection3).and(pKVcollection4);
    PCollection<KV<String, String>> mergedList = pCollectionList.apply(Flatten.<KV<String,String>>pCollections());
    PCollection<KV<String, Iterable<String>>> pColGroupByKey = mergedList.apply(GroupByKey.create());
    PCollection<KV<String, RankedPage>> job2in = pColGroupByKey.apply(ParDo.of(new Job1Finalizer()));
    PCollection<KV<String, RankedPage>> job2out = null;

    int iterations = 50;
    for (int i = 1; i <= iterations; i++) {
      PCollection<KV<String, RankedPage>> job2Mapper = job2in.apply(ParDo.of(new Job2Mapper()));

      PCollection<KV<String, Iterable<RankedPage>>> job2MapperGrpByKey = job2Mapper.apply(GroupByKey.create());

      job2out = job2MapperGrpByKey.apply(ParDo.of(new Job2Updater()));
      job2in = job2out;
    }

    PCollection<String> pColctnStringLists = job2out.apply(
      MapElements.into(
          TypeDescriptors.strings()).via(
              kvtoString -> kvtoString.toString()));
  pColctnStringLists.apply(TextIO.write().to("BalajiPR"));

  p.run().waitUntilFinish();
  }

  public static PCollection<KV<String,String>> BalajiMapper01(Pipeline p, String filename, String dataFolder){
   
    String newdataPath = dataFolder + "/" + filename;
     PCollection<String> pcolInput = p.apply(TextIO.read().from(newdataPath));
     PCollection<String> pcollinkLines = pcolInput.apply(Filter.by((String line) -> line.startsWith("[")));
     PCollection<String> pcolLinks = pcollinkLines.apply(MapElements.into((TypeDescriptors.strings()))
     .via((String linkLine) ->linkLine.substring(linkLine.indexOf("(")+1, linkLine.length()-1)));
     PCollection<KV<String,String>> pColKVPairs =  pcolLinks.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
     .via((String outLink) -> KV.of(filename,outLink)));
    return pColKVPairs;
  }

}