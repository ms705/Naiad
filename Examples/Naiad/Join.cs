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

namespace Microsoft.Research.Naiad.Examples.Join
{

  public class Join : Example
  {
    public struct IntPair : IEquatable<IntPair> {
      public int s;
      public int t;

      public bool Equals(IntPair that) {
        return s == that.s && t == that.t;
      }

      public int CompareTo(IntPair that) {
        if (this.s != that.s)
          return this.s - that.s;

        return this.t - that.t;
      }

      public override int GetHashCode() {
        return 47 * s + 36425232 * t;
      }

      public override string ToString() {
        return String.Format("{0} {1}", s, t);
      }

      public IntPair(int ss, int tt) {
        s = ss;
        t = tt;
      }
    }

    public struct IntTriple : IEquatable<IntTriple> {
      public int first;
      public int second;
      public int third;

      public bool Equals(IntTriple that) {
        return first == that.first && second == that.second && third == that.third;
      }

      public int CompareTo(IntTriple that) {
        if (this.first != that.first)
          return this.first - that.first;

        if (this.second != that.second)
          return this.second - that.second;

        return this.third - that.third;
      }

      // Embarassing hashcodes
      public override int GetHashCode() {
        return first + 1234347 * second + 4311 * third;
      }

      public override string ToString() {
        return String.Format("{0} {1} {2}", first, second, third);
      }

      public IntTriple(int x, int y, int z) {
        first = x; second = y; third = z;
      }
    }

    public IEnumerable<IntPair> read_rel(string filename) {
      StreamReader reader = File.OpenText(filename);
      for (; !reader.EndOfStream; ) {
        var elements = reader.ReadLine().Split(' ');
        yield return new IntPair(Convert.ToInt32(elements[0]),
                                 Convert.ToInt32(elements[1]));
      }
    }

    public string Usage {get {return "<local path prefix>";} }

    public void Execute(string[] args) {
      using (var computation = NewComputation.FromArgs(ref args)) {
        var left_input = new BatchedDataSource<string>();
        var right_input = new BatchedDataSource<string>();
        var left = computation.NewInput(left_input).SelectMany(filename => read_rel(filename));
        var right = computation.NewInput(right_input).SelectMany(filename => read_rel(filename));

        var output = left.Join(right,
               left_row => left_row.t,
               right_row => right_row.t,
               (left_row, right_row) => new IntTriple(left_row.s, left_row.t, right_row.s));

        int minThreadId = computation.Configuration.ProcessID *
          computation.Configuration.WorkerCount;
        StreamWriter[] file_output =
          new StreamWriter[computation.Configuration.WorkerCount];
        for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
          int j = minThreadId + i;
          file_output[i] = new StreamWriter(args[1] + "/join" + j + ".out");
        }
        output.Subscribe((i, l) => { foreach (var element in l) file_output[i - minThreadId].WriteLine(element); });
        computation.Activate();
        left_input.OnCompleted(args[1] + "/join_left" + computation.Configuration.ProcessID + ".in");
        right_input.OnCompleted(args[1] + "/join_right" + computation.Configuration.ProcessID + ".in");
        computation.Join();
        for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
          file_output[i].Close();
        }
      }
    }

    public string Help {
      get {
        return "Join <local path prefix>";
      }
    }
  }

}
