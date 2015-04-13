using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Dataflow.Iteration;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Frameworks.Reduction;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
using Microsoft.Research.Naiad.Input;

namespace Microsoft.Research.Naiad.Examples.PageRankV2 {

  public class PageRankV2 : Example
  {

    public IEnumerable<IntInt> read_edges(string filename)
    {
      StreamReader reader = File.OpenText(filename);
      for (; !reader.EndOfStream; )
      {
        var elements = reader.ReadLine().Split(' ');
        yield return new IntInt(Convert.ToInt32(elements[0]),Convert.ToInt32(elements[1]));
      }
    }

    public IEnumerable<IntDouble> read_pr(string filename) {
      StreamReader reader = File.OpenText(filename);
      for (; !reader.EndOfStream; ) {
        var elements = reader.ReadLine().Split(' ');
        yield return new IntDouble(Convert.ToInt32(elements[0]),Convert.ToDouble(elements[1]));
      }
    }

    public struct Reducernode_cnt : IAddable<Reducernode_cnt>
    {

      public int value0;

      public Reducernode_cnt(int value0)
      {
        this.value0 = value0;
      }

      public Reducernode_cnt Add(Reducernode_cnt other)
      {
        value0 += other.value0;
        return this;
      }

    }

    public struct Reducerpr1 : IAddable<Reducerpr1>
    {

      public double value0;

      public Reducerpr1(double value0)
      {
        this.value0 = value0;
      }

      public Reducerpr1 Add(Reducerpr1 other)
      {
        value0 += other.value0;
        return this;
      }

    }


    public string Usage {get {return "";} }

    public static Stream<IntDouble, IterationIn<Epoch>> PageRankStep(
        Stream<IntDouble, IterationIn<Epoch>> pr,
        Stream<IntIntInt, Epoch> edgescnt,
        LoopContext<Epoch> lc)
    {
      return edgescnt.TransmitAlong1(pr)
        .Select(row => new IntDouble(row.second, row.fourth / row.third))
        .GenericAggregator(row => row.first,
                           row => new Reducepr1(row.second))
        .Select(row => new IntDouble(row.First, row.Second.value0 * 0.85 + 0.15));
    }

    public void Execute(string[] args) {
      using (var computation = Naiad.NewComputation.FromArgs(ref args)) {
        var edges_input = new BatchedDataSource<string>();
        var pr_input = new BatchedDataSource<string>();
        var edges = computation.NewInput(edges_input).SelectMany(line => read_edges(line));
        var pr = graph.NewInput(pr_input).SelectMany(line => read_pr(line));
        var node_cnt_tmp =
          edges.GenericAggregator(row => row.first,
                                  row => new Reducernode_cnt(1));
        var node_cnt = node_cnt_tmp.Select(row => { Reducernode_cnt aggVal = (Reducernode_cnt)row.Second ; return new IntInt(row.First, aggVal.value0); });
        var edgescnt = edges.Join(node_cnt,
                                  (IntInt left) => left.first,
                                  (IntInt right) => right.first,
                                  (IntInt left, IntInt right) => new IntIntInt(left.first, left.second , right.second));

        pr = pr.IterateAndAccumulate((lc, deltas) => deltas.PageRankStep(edgescnt, lc),
                                     x => x.first,
                                     5,
                                     "PageRank");

        int minThreadId = computation.Configuration.ProcessID *
          computation.Configuration.WorkerCount;

        StreamWriter[] file_pr = new StreamWriter[computation.Configuration.WorkerCount];
        for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
          int j = minThreadId + i;
          file_pr[i] = new StreamWriter("pr" + j + ".out");
        }

        pr.Subscribe((i, l) => { foreach (var element in l) file_pr[i - minThreadId].WriteLine(element); });

        computation.Activate();
        edges_input.OnCompleted("/tmp//edges/edges" + computation.Configuration.ProcessID + ".in");
        pr_input.OnCompleted("/tmp/pr/pr" + computation.Configuration.ProcessID + ".in");
        computation.Join();
        for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
          file_pr[i].Close();
        }
      }
    }

    public string Help {
      get {
        return "PageRankV2";
      }
    }
  }

}