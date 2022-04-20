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
package edu.nwmissouri.deccanchargers;

import java.util.ArrayList;

// beam-playground:
//   name: MinimalWordCount
//   description: An example that counts words in Shakespeare's works.
//   multifile: false
//   pipeline_options:
//   categories:
//     - Combiners
//     - Filtering
//     - IO
//     - Core Transforms

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
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

/**
 * An example that counts words in Shakespeare.
 *
 * <p>
 * This class, {@link MinimalWordCount}, is the first in a series of four
 * successively more
 * detailed 'word count' examples. Here, for simplicity, we don't show any
 * error-checking or
 * argument processing, and focus on construction of the pipeline, which chains
 * together the
 * application of core transforms.
 *
 * <p>
 * Next, see the {@link WordCount} pipeline, then the
 * {@link DebuggingWordCount}, and finally the
 * {@link WindowedWordCount} pipeline, for more detailed examples that introduce
 * additional
 * concepts.
 *
 * <p>
 * Concepts:
 *
 * <pre>
 *   1. Reading data from text files
 *   2. Specifying 'inline' transforms
 *   3. Counting items in a PCollection
 *   4. Writing data to text files
 * </pre>
 *
 * <p>
 * No arguments are required to run this pipeline. It will be executed with the
 * DirectRunner. You
 * can see the results in the output files in your current working directory,
 * with names like
 * "wordcounts-00001-of-00005. When running on a distributed service, you would
 * use an appropriate
 * file service.
 */
public class PageRankYaswant {

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

        static class Job1Mapper extends DoFn<KV<String, RankedPage>, KV<String, RankedPage>> {
        }

        static class Job1Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
        }

        public static void main(String[] args) {

                PipelineOptions options = PipelineOptionsFactory.create();
                Pipeline Y = Pipeline.create(options);
                String dataFolder = "web04";
                String dataFile = "go.md";
                PCollection<KV<String, String>> L1 = yaswantMapper(Y, dataFolder, "go.md");
                PCollection<KV<String, String>> L2 = yaswantMapper(Y, dataFolder, "python.md");
                PCollection<KV<String, String>> L3 = yaswantMapper(Y, dataFolder, "java.md");
                PCollection<KV<String, String>> L4 = yaswantMapper(Y, dataFolder, "README.md");
                PCollectionList<KV<String, String>> PList = PCollectionList.of(L1).and(L2).and(L3).and(L4);
                PCollection<KV<String, String>> mergedList = PList.apply(Flatten.<KV<String, String>>pCollections());
                PCollection<String> PStr = mergedList.apply(
                                MapElements.into(
                                                TypeDescriptors.strings())
                                                .via((mergeOut) -> mergeOut.toString()));
                PStr.apply(TextIO.write().to("yaswantout"));
                Y.run().waitUntilFinish();
        }

        private static PCollection<KV<String, String>> yaswantMapper(Pipeline p, String dataFolder, String dataFile) {
                String dataLocation = dataFolder + "/" + dataFile;
                PCollection<String> pcolInputLines = p.apply(TextIO.read().from(dataLocation));
                PCollection<String> pcolLinkLines = pcolInputLines
                                .apply(Filter.by((String line) -> line.startsWith("[")));
                PCollection<String> pcolLinkPages = pcolLinkLines.apply(MapElements.into(TypeDescriptors.strings())
                                .via(
                                                (String linkline) -> linkline.substring(linkline.indexOf("(") + 1,
                                                                linkline.length() - 1)));
                PCollection<KV<String, String>> pcolKVpairs = pcolLinkPages.apply(MapElements
                                .into(
                                                TypeDescriptors.kvs(
                                                                TypeDescriptors.strings(), TypeDescriptors.strings()))
                                .via(outlink -> KV.of(dataFile, outlink)));
                return pcolKVpairs;
        }
}