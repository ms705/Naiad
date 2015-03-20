using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Frameworks.Reduction;
using Microsoft.Research.Naiad.Input;

namespace Microsoft.Research.Naiad.Examples.SSSP {

  public class SSSP : Example {

    public struct IntIntInt : IEquatable<IntIntInt> {
      public int first;
      public int second;
      public int third;

      public bool Equals(IntIntInt that) {
        return first == that.first && second == that.second && third == that.third;
      }

      public int CompareTo(IntIntInt that) {
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

      public override string ToString() {
        return String.Format("{0} {1} {2}", first, second, third);
      }

      public IntIntInt(int x, int y, int z) {
        first = x;
        second = y;
        third = z;
      }
    }

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

    public IEnumerable<IntInt> ReadVertices(string filename)
    {
      if (File.Exists(filename)) {
        var file = File.OpenText(filename);
        while (!file.EndOfStream) {
          var elements = file.ReadLine().Split(' ');
          yield return new IntInt(Convert.ToInt32(elements[0]),
                                  Convert.ToInt32(elements[1]));
        }
      } else {
        Console.WriteLine("File not found! {0}", filename);
      }
    }

    public IEnumerable<IntIntInt> ReadEdges(string filename)
    {
      if (File.Exists(filename)) {
        var file = File.OpenText(filename);
        while (!file.EndOfStream) {
          var elements = file.ReadLine().Split(' ');
          yield return new IntIntInt(Convert.ToInt32(elements[0]),
                                     Convert.ToInt32(elements[1]),
                                     Convert.ToInt32(elements[2]));
        }
      } else {
        Console.WriteLine("File not found! {0}", filename);
      }
    }

    public IEnumerable<IntInt> MinCosts(int id, IEnumerable<IntInt> costs) {
      int minCost = int.MaxValue;
      foreach (var cost in costs) {
        minCost = Math.Min(minCost, cost.second);
      }
      yield return new IntInt(id, minCost);
    }

    public Stream<IntInt, T> SSSPIter<T>(
        Stream<IntIntInt, T> edges_in,
        Stream<IntInt, T> vertices_in)
      where T: Time<T> {
      return edges_in.Join(vertices_in,
                    (IntIntInt edge) => edge.first,
                    (IntInt vertex) => vertex.first,
                    (IntIntInt edge, IntInt vertex) =>
                           new IntInt(edge.second, vertex.second + edge.third))
        .Union(vertices_in)
        .Min(row => row.first,
             row => row.second)
        .Select(row => new IntInt(row.First, row.Second));
//        .GroupBy(row => row.first,
//                 (id, costs) => MinCosts(id, costs));
    }

    public string Usage {get {return "<graph> <local input path prefix>";} }

    public void Execute(string[] args) {
      using (var computation = Naiad.NewComputation.FromArgs(ref args)) {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        var edges = new BatchedDataSource<string>();
        var vertices = new BatchedDataSource<string>();

        var edges_in = computation.NewInput(edges)
          .SelectMany(file => ReadEdges(file));
        var vertices_in = computation.NewInput(vertices)
          .SelectMany(file => ReadVertices(file));

        for (int i = 0; i < 5; ++i) {
          vertices_in = SSSPIter(edges_in, vertices_in);
        }

        int minThreadId = computation.Configuration.ProcessID *
          computation.Configuration.WorkerCount;
        StreamWriter[] file_out = new StreamWriter[computation.Configuration.WorkerCount];
        for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
          int j = minThreadId + i;
          file_out[i] = new StreamWriter(args[2] + "/dij_vertices" + j + ".out");
        }

        vertices_in.Subscribe((i, l) => { foreach (var element in l) file_out[i - minThreadId].WriteLine(element); });

        computation.Activate();
        edges.OnCompleted(args[2] + "/sssp_" + args[1] + "_edges" + computation.Configuration.ProcessID + ".in");
        vertices.OnCompleted(args[2] + "/sssp_" + args[1] + "_vertices" + computation.Configuration.ProcessID + ".in");
        computation.Join();
        for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
          file_out[i].Close();
        }
      }
    }

    public string Help {
      get {
        return "SSSP <graph> <local input path prefix>";
      }
    }

  }

}
