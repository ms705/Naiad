using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Frameworks.Reduction;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
using Microsoft.Research.Naiad.Input;

namespace Microsoft.Research.Naiad.Examples.PageRank
{

  public class PageRank : Example {

    public struct IntInt : IEquatable<IntInt> {
      public int first;
      public int second;

      public bool Equals(IntInt that) {
        return first == that.first && second == that.second;
      }

      public int CompareTo(IntInt that) {
        if (this.first != that.first)
          return this.first - that.first;

        return this.second - that.second;
      }

      public override int GetHashCode() {
        return 47 * first + 36425232 * second;
      }

      public override string ToString() {
        return String.Format("{0} {1}", first, second);
      }

      public IntInt(int ss, int tt) {
        first = ss;
        second = tt;
      }

    }

    public struct IntDouble : IEquatable<IntDouble> {
      public int first;
      public double second;

      public bool Equals(IntDouble that) {
        return first == that.first && second == that.second;
      }

      public int CompareTo(IntDouble that) {
        if (first != that.first)
          return first - that.first;
        if (second < that.second) {
          return -1;
        } else if (second > that.second) {
          return 1;
        } else {
          return 0;
        }
      }

      public override int GetHashCode() {
        return 47 * first + 36425232 * second.GetHashCode();
      }

      public override string ToString() {
        return String.Format("{0} {1}", first, second);
      }

      public IntDouble(int x, double y) {
        first = x;
        second = y;
      }

    }

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


    public struct IntIntInt : IEquatable<IntIntInt>
    {
      public int first;
      public int second;
      public int third;

      public bool Equals(IntIntInt that)
      {
        return first == that.first && second == that.second && third == that.third;
      }

      public int CompareTo(IntIntInt that)
      {
        if (this.first != that.first)
          return this.first - that.first;

        if (this.second != that.second)
          return this.second - that.second;

        return this.third - that.third;
      }

      // Embarassing hashcodes
      public override int GetHashCode()
      {
        return first + 1234347 * second + 4311 * third;
      }

      public override string ToString()
      {
        return String.Format("{0} {1} {2}", first, second, third);
      }

      public IntIntInt(int x, int y, int z)
      {
        first = x;
        second = y;
        third = z;
      }
    }

    public struct IntIntIntDouble : IEquatable<IntIntIntDouble> {
      public int first;
      public int second;
      public int third;
      public double fourth;

      public bool Equals(IntIntIntDouble that) {
        return first == that.first && second == that.second &&
          third == that.third && Math.Abs(fourth - that.fourth) < 0.0000001;
      }

      public int CompareTo(IntIntIntDouble that) {
        if (this.first != that.first)
          return this.first - that.first;

        if (this.second != that.second)
          return this.second - that.second;

        if (this.third != that.third)
          return this.third - that.third;

        return this.fourth - that.fourth < 0 ? -1 : 1;
      }

      // Embarassing hashcodes
      public override int GetHashCode() {
        return first + 1234347 * second + 4311 * third + fourth.GetHashCode();
      }

      public override string ToString() {
        return String.Format("{0} {1} {2} {3}", first, second, third, fourth);
      }

      public IntIntIntDouble(int x, int y, int z, double zz) {
        first = x;
        second = y;
        third = z;
        fourth = zz;
      }
    }

    public string Usage {get {return "<graph> <local path prefix>";} }

    public void Execute(string[] args) {
      using (var computation = NewComputation.FromArgs(ref args)) {
        var edges_input = new BatchedDataSource<string>();
        var pr_input = new BatchedDataSource<string>();
        var edges = computation.NewInput(edges_input).SelectMany(line => read_edges(line));
        var pr = computation.NewInput(pr_input).SelectMany(line => read_pr(line));
        var node_cnt_tmp =
          edges.GenericAggregator(row => row.first,
                                  row => new Reducernode_cnt(1));
        var node_cnt = node_cnt_tmp.Select(row => { Reducernode_cnt aggVal = (Reducernode_cnt)row.Second ; return new IntInt(row.First, aggVal.value0); });
        var edgescnt = edges.Join(node_cnt,
                                  (IntInt left) => left.first,
                                  (IntInt right) => right.first,
                                  (IntInt left, IntInt right) => new IntIntInt(left.first, left.second , right.second));
        for (int i = 0; i < 5; ++i) {
          var edgespr = edgescnt.Join(pr,
                                      (IntIntInt left) => left.first,
                                      (IntDouble right) => right.first,
                                      (IntIntInt left, IntDouble right) => new IntIntIntDouble(left.first, left.second, left.third , right.second));
          var rankcnt = edgespr.Select(row => new IntIntIntDouble(row.first,row.second,row.third,row.fourth / row.third));
          var links = rankcnt.Select(row => new IntDouble(row.second,row.fourth));
          var pr1_tmp = links.GenericAggregator(row => row.first,
                                                row => new Reducerpr1(row.second));
          var pr1 = pr1_tmp.Select(row => { Reducerpr1 aggVal = (Reducerpr1)row.Second ; return new IntDouble(row.First, aggVal.value0); });
          var pr2 = pr1.Select(row => new IntDouble(row.first,0.85 * row.second));
          pr = pr2.Select(row => new IntDouble(row.first,0.15 + row.second));
        }

        int minThreadId = computation.Configuration.ProcessID * computation.Configuration.WorkerCount;

        StreamWriter[] file_pr = new StreamWriter[computation.Configuration.WorkerCount];
        for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
          int j = minThreadId + i;
          file_pr[i] = new StreamWriter(args[2] + "/pagerank_" + args[1] + j + ".out");
        }

        pr.Subscribe((i, l) => { foreach (var element in l) file_pr[i - minThreadId].WriteLine(element); });

        computation.Activate();
        edges_input.OnCompleted(args[2] + "/pagerank_" + args[1] + "_edges" + computation.Configuration.ProcessID + ".in");
        pr_input.OnCompleted(args[2] + "/pagerank_" + args[1] + "_vertices" + computation.Configuration.ProcessID + ".in");
        computation.Join();
        for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
          file_pr[i].Close();
        }
      }
    }

    public string Help {
      get {
        return "PageRank <graph> <local path prefix>";
      }
    }

  }
}
