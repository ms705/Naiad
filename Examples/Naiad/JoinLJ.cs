using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using System.Text;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Input;

namespace Microsoft.Research.Naiad.Examples.JoinLJ
{

  public class JoinLJ : Example {

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

    public struct IntIntDouble : IEquatable<IntIntDouble> {
      public int first;
      public int second;
      public double third;

      public bool Equals(IntIntDouble that) {
        return first == that.first && second == that.second &&
          Math.Abs(third - that.third) < 0.0000001;
      }

      public int CompareTo(IntIntDouble that) {
        if (this.first != that.first)
          return this.first - that.first;

        if (this.second != that.second)
          return this.second - that.second;
        return this.third - that.third < 0 ? -1 : 1;
      }

      // Embarassing hashcodes
      public override int GetHashCode() {
        return first + 1234347 * second + 4311 * third.GetHashCode();
      }

      public override string ToString() {
        return String.Format("{0} {1} {2}", first, second, third);
      }

      public IntIntDouble(int x, int y, double z) {
        first = x;
        second = y;
        third = z;
      }
    }

    public IEnumerable<IntPair> read_edges(string filename) {
      StreamReader reader = File.OpenText(filename);
      for (; !reader.EndOfStream; ) {
        var elements = reader.ReadLine().Split(' ');
        yield return new IntPair(Convert.ToInt32(elements[0]),
                                 Convert.ToInt32(elements[1]));
      }
    }

    public IEnumerable<IntDouble> read_vertices(string filename) {
      StreamReader reader = File.OpenText(filename);
      for (; !reader.EndOfStream; ) {
        var elements = reader.ReadLine().Split(' ');
        yield return new IntDouble(Convert.ToInt32(elements[0]),
                                   Convert.ToDouble(elements[1]));
      }
    }

    public string Usage {get {return "";} }

    public void Execute(string[] args) {
      using (var computation = NewComputation.FromArgs(ref args)) {
        var left_input = new BatchedDataSource<string>();
        var right_input = new BatchedDataSource<string>();
        var left = computation.NewInput(left_input).SelectMany(filename => read_edges(filename));
        var right = computation.NewInput(right_input).SelectMany(filename => read_vertices(filename));

        var output = left.Join(right,
                  left_row => left_row.s,
                  right_row => right_row.first,
                  (left_row, right_row) => new IntIntDouble(left_row.s,
                                                            left_row.t, right_row.second));

        int minThreadId = computation.Configuration.ProcessID *
          computation.Configuration.WorkerCount;
        StreamWriter[] file_output = new StreamWriter[computation.Configuration.WorkerCount];
        for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
          int j = minThreadId + i;
          file_output[i] = new StreamWriter(args[3] + j + ".out");
        }
        output.Subscribe((i, l) => { foreach (var element in l) file_output[i - minThreadId].WriteLine(element); });
        computation.Activate();
        left_input.OnCompleted(args[1]);
        right_input.OnCompleted(args[2]);

        computation.Join();
        for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
          file_output[i].Close();
        }
      }
    }

    public string Help {
      get {
        return "JoinLJ <left_input> <right_input>";
      }
    }
  }

}
