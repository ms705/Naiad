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

namespace Microsoft.Research.Naiad.Examples.KMeans
{

  public class KMeans : Example {

    public struct DoubleDouble : IEquatable<DoubleDouble>, IComparable<DoubleDouble> {
      public double first;
      public double second;

      public bool Equals(DoubleDouble that) {
        return Math.Abs(first - that.first) < 0.0000001 &&
          Math.Abs(second - that.second) < 0.0000001;
      }

      public int CompareTo(DoubleDouble that) {
        if (Math.Abs(this.first - that.first) > 0.0000001)
          return this.first - that.first < 0 ? -1 : 1;
        if (Math.Abs(this.second - that.second) < 0.0000001)
          return 0;
        return this.second - that.second < 0 ? -1 : 1;
      }

      public override int GetHashCode() {
        return 47 * first.GetHashCode() + 36425232 * second.GetHashCode();
      }

      public override string ToString() {
        return String.Format("{0} {1}", first, second);
      }

      public DoubleDouble(double x, double y) {
        first = x;
        second = y;
      }

    }

    public struct DoubleDoubleDouble : IEquatable<DoubleDoubleDouble> {
      public double first;
      public double second;
      public double third;

      public bool Equals(DoubleDoubleDouble that) {
        return Math.Abs(first - that.first) < 0.0000001 &&
          Math.Abs(second - that.second) < 0.0000001 &&
          Math.Abs(third - that.third) < 0.0000001;
      }

      public int CompareTo(DoubleDoubleDouble that) {
        if (Math.Abs(this.first - that.first) > 0.0000001)
          return this.first - that.first < 0 ? -1 : 1;
        if (Math.Abs(this.second - that.second) > 0.0000001)
          return this.second - that.second < 0 ? -1 : 1;
        if (Math.Abs(this.third - that.third) < 0.0000001)
          return 0;
        return this.third - that.third < 0 ? -1 : 1;
      }

      public override int GetHashCode() {
        return 47 * first.GetHashCode() + 36425232 * second.GetHashCode() + 17 *
          third.GetHashCode();
      }

      public override string ToString() {
        return String.Format("{0} {1} {2}", first, second, third);
      }

      public DoubleDoubleDouble(double x, double y, double z) {
        first = x;
        second = y;
        third = z;
      }

    }

    public struct DoubleDoubleDoubleDouble : IEquatable<DoubleDoubleDoubleDouble> {
      public double first;
      public double second;
      public double third;
      public double fourth;

      public bool Equals(DoubleDoubleDoubleDouble that) {
        return Math.Abs(first - that.first) < 0.0000001 &&
          Math.Abs(second - that.second) < 0.0000001 &&
          Math.Abs(third - that.third) < 0.0000001 &&
          Math.Abs(fourth - that.fourth) < 0.0000001;
      }

      public int CompareTo(DoubleDoubleDoubleDouble that) {
        if (Math.Abs(this.first - that.first) > 0.0000001)
          return this.first - that.first < 0 ? -1 : 1;
        if (Math.Abs(this.second - that.second) > 0.0000001)
          return this.second - that.second < 0 ? -1 : 1;
        if (Math.Abs(this.third - that.third) > 0.0000001)
          return this.third - that.third < 0 ? -1 : 1;
        if (Math.Abs(this.fourth - that.fourth) < 0.0000001)
          return 0;
        return this.fourth - that.fourth < 0 ? -1 : 1;
      }

      public override int GetHashCode() {
        return 47 * first.GetHashCode() + 36425232 * second.GetHashCode() + 17 *
          third.GetHashCode() + fourth.GetHashCode();
      }

      public override string ToString() {
        return String.Format("{0} {1} {2} {3}", first, second, third, fourth);
      }

      public DoubleDoubleDoubleDouble(double x, double y, double z, double xx) {
        first = x;
        second = y;
        third = z;
        fourth = xx;
      }

      public static bool operator <(DoubleDoubleDoubleDouble l,
                                    DoubleDoubleDoubleDouble r) {
        return l.CompareTo(r) < 0;
      }

      public static bool operator >(DoubleDoubleDoubleDouble l,
                                    DoubleDoubleDoubleDouble r) {
        return l.CompareTo(r) > 0;
      }

    }

    public IEnumerable<DoubleDouble> ReadPoints(string filename) {
      if (File.Exists(filename)) {
        var file = File.OpenText(filename);
        while (!file.EndOfStream) {
          var elements = file.ReadLine().Split(' ');
          yield return new DoubleDouble(Double.Parse(elements[0]),
                                        Double.Parse(elements[1]));
        }
      } else {
        Console.WriteLine("File not found! {0}", filename);
      }
    }

    public IEnumerable<DoubleDoubleDouble> ReadClusters(string filename) {
      if (File.Exists(filename)) {
        var file = File.OpenText(filename);
        while (!file.EndOfStream) {
          var elements = file.ReadLine().Split(' ');
          yield return new DoubleDoubleDouble(Double.Parse(elements[0]),
                                              Double.Parse(elements[1]),
                                              Double.Parse(elements[2]));
        }
      } else {
        Console.WriteLine("File not found! {0}", filename);
      }
    }

    public IEnumerable<DoubleDoubleDouble> MinClst(DoubleDouble point,
                                                   IEnumerable<DoubleDoubleDoubleDouble> pntclsts) {
      double minDist = Double.MaxValue;
      DoubleDoubleDoubleDouble minRow =
        new DoubleDoubleDoubleDouble(0, 0, 0, 0);
      foreach (var pntclst in pntclsts) {
        if (pntclst.third < minDist) {
          minDist = pntclst.third;
          minRow = pntclst;
        }
      }
      yield return new DoubleDoubleDouble(point.first, point.second, minRow.fourth);
    }

    public IEnumerable<DoubleDoubleDouble> CalcAvg(Double clst_id,
                                                   IEnumerable<DoubleDoubleDouble> clsts) {
      double sumx = 0;
      double sumy = 0;
      double cnt = 0;
      foreach (var clst in clsts) {
        sumx += clst.first;
        sumy += clst.second;
        cnt += 1;
      }
      yield return new DoubleDoubleDouble(sumx / cnt, sumy / cnt, clst_id);
    }

    public Stream<DoubleDoubleDouble, T> KMeansIter<T>(
        Stream<DoubleDouble, T> points_in,
        Stream<DoubleDoubleDouble, T> clusters_in)
      where T: Time<T> {
      var pnt_clsts = points_in.Join(clusters_in,
                      (DoubleDouble point) => 1,
                      (DoubleDoubleDouble cluster) => 1,
                      (DoubleDouble point, DoubleDoubleDouble cluster) =>
                      new DoubleDoubleDoubleDouble(point.first,
                                                   point.second,
                                                   (cluster.first - point.first) * (cluster.first - point.first) + (cluster.second - point.second) * (cluster.second - point.second),
                                                   cluster.third));
      return pnt_clsts.Min(row => new DoubleDouble(row.first, row.second),
                    row => new DoubleDouble(row.third, row.fourth))
        .Select(row => new DoubleDoubleDouble(row.First.first, row.First.second, row.Second.second))
        .GroupBy(row => row.third,
                 (clst_id, pnts) => CalcAvg(clst_id, pnts));
    }

    public string Usage {get {return "";} }

    public void Execute(string[] args) {
      using (var computation = Naiad.NewComputation.FromArgs(ref args)) {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        var points = new BatchedDataSource<string>();
        var clusters = new BatchedDataSource<string>();

        var points_in = computation.NewInput(points).SelectMany(file => ReadPoints(file));
        var clusters_in = computation.NewInput(clusters).SelectMany(file => ReadClusters(file));

        // var output = clusters_in.IterateAndAccumulate((lc, deltas) =>
        //      deltas.Join(lc.EnterLoop(points_in),
        //               (DoubleDoubleDouble cluster) => 1,
        //               (DoubleDouble point) => 1,
        //               (DoubleDoubleDouble cluster, DoubleDouble point) =>
        //                new DoubleDoubleDoubleDouble(point.first,
        //                                             point.second,
        //                                             (cluster.first - point.first) *
        //                                             (cluster.first - point.first) +
        //                                             (cluster.second - point.second) *
        //                                             (cluster.second - point.second),
        //                                             cluster.third))
        //        .GroupBy(row => new DoubleDouble(row.first, row.second),
        //                 (pnts, pntclsts) => MinClst(pnts, pntclsts)),
        //        .GroupBy(row => row.third,
        //                 (clst_id, pnts) => CalcAvg(clst_id, pnts)),
        //     null, 5, "KMeans");

        for (int i = 0; i < 5; ++i) {
          clusters_in = KMeansIter(points_in, clusters_in);
        }

        int minThreadId = computation.Configuration.ProcessID *
          computation.Configuration.WorkerCount;
        StreamWriter[] file_out = new StreamWriter[computation.Configuration.WorkerCount];
        for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
          int j = minThreadId + i;
          file_out[i] = new StreamWriter("/tmp/clusters/clusters" + j + ".out");
        }

        clusters_in.Subscribe((i, l) => { foreach (var element in l) file_out[i - minThreadId].WriteLine(element); });
        computation.Activate();
        points.OnCompleted(args[1]);
        clusters.OnCompleted(args[2]);
        computation.Join();
        for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
          file_out[i].Close();
        }
      }
    }

    public string Help {
      get {
        return "KMeans <points_file> <clusters_file>";
      }
    }
  }

}
