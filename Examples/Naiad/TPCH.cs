using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Frameworks.Reduction;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
using Microsoft.Research.Naiad.Input;

namespace Microsoft.Research.Naiad.Examples.TPCH {

  public class TPCH : Example {

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

    public struct IntStringString : IEquatable<IntStringString> {
      public int first;
      public string second;
      public string third;

      public bool Equals(IntStringString that) {
        return first == that.first && second.Equals(that.second) &&
          third.Equals(that.third);
      }

      public int CompareTo(IntStringString that) {
        if (this.first != that.first)
          return this.first - that.first;

        if (!this.second.Equals(that.second))
          return this.second.CompareTo(that.second);

        return this.third.CompareTo(that.third);
      }

      // Embarassing hashcodes
      public override int GetHashCode() {
        return first + 1234347 * second.GetHashCode() + 4311 * third.GetHashCode();
      }

      public override string ToString() {
        return String.Format("{0} {1} {2}", first, second, third);
      }

      public IntStringString(int x, string y, string z) {
        first = x;
        second = y;
        third = z;
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

    public struct IntIntDoubleStringString : IEquatable<IntIntDoubleStringString> {
      public int first;
      public int second;
      public double third;
      public string fourth;
      public string fifth;

      public bool Equals(IntIntDoubleStringString that) {
        return first == that.first && second == that.second &&
          Math.Abs(third - that.third) < 0.0000001 &&
          fourth.Equals(that.fourth) && fifth.Equals(that.fifth);
      }

      public int CompareTo(IntIntDoubleStringString that) {
        if (this.first != that.first)
          return this.first - that.first;

        if (this.second != that.second)
          return this.second - that.second;

        if (Math.Abs(this.third - that.third) > 0.0000001)
          return this.third - that.third < 0 ? -1 : 1;

        if (!this.fourth.Equals(that.fourth))
          return this.fourth.CompareTo(that.fourth);

        return this.fifth.CompareTo(that.fifth);
      }

      // Embarassing hashcodes
      public override int GetHashCode() {
        return first + 1234347 * second + 4311 * third.GetHashCode() + 42 * fourth.GetHashCode() +
          17 * fifth.GetHashCode();
      }

      public override string ToString() {
        return String.Format("{0} {1} {2} {3} {4}", first, second, third, fourth, fifth);
      }

      public IntIntDoubleStringString(int x, int y, double z, string xx, string yy) {
        first = x;
        second = y;
        third = z;
        fourth = xx;
        fifth = yy;
      }
    }

    public struct IntIntDoubleStringStringDouble : IEquatable<IntIntDoubleStringStringDouble> {
      public int first;
      public int second;
      public double third;
      public string fourth;
      public string fifth;
      public double sixth;

      public bool Equals(IntIntDoubleStringStringDouble that) {
        return first == that.first && second == that.second &&
          Math.Abs(third - that.third) < 0.0000001 &&
          fourth.Equals(that.fourth) && fifth.Equals(that.fifth) &&
          Math.Abs(sixth - that.sixth) < 0.0000001;
      }

      public int CompareTo(IntIntDoubleStringStringDouble that) {
        if (this.first != that.first)
          return this.first - that.first;

        if (this.second != that.second)
          return this.second - that.second;

        if (Math.Abs(this.third - that.third) > 0.0000001)
          return this.third - that.third < 0 ? -1 : 1;

        if (!this.fourth.Equals(that.fourth))
          return this.fourth.CompareTo(that.fourth);

        if (!this.fifth.Equals(that.fifth))
          return this.fifth.CompareTo(that.fifth);

        return this.sixth.CompareTo(that.sixth);
      }

      // Embarassing hashcodes
      public override int GetHashCode() {
        return first + 1234347 * second + 4311 * third.GetHashCode() + 42 * fourth.GetHashCode() +
          17 * fifth.GetHashCode() + 13 * sixth.GetHashCode();
      }

      public override string ToString() {
        return String.Format("{0} {1} {2} {3} {4} {5}", first, second,
                             third, fourth, fifth, sixth);
      }

      public IntIntDoubleStringStringDouble(int x, int y, double z, string xx,
                                            string yy, double xxx) {
        first = x;
        second = y;
        third = z;
        fourth = xx;
        fifth = yy;
        sixth = xxx;
      }
    }

    public IEnumerable<IntStringString> read_part(string filename) {
      StreamReader reader = File.OpenText(filename);
      for (; !reader.EndOfStream; ) {
        var elements = reader.ReadLine().Split(' ');
        yield return new IntStringString(Convert.ToInt32(elements[0]),
                                         elements[3], elements[6]);
      }
    }

    public IEnumerable<IntIntDouble> read_lineitem(string filename) {
      StreamReader reader = File.OpenText(filename);
      for (; !reader.EndOfStream; ) {
        var elements = reader.ReadLine().Split(' ');
        yield return new IntIntDouble(Convert.ToInt32(elements[1]),
                                      Convert.ToInt32(elements[4]),
                                      Convert.ToDouble(elements[5]));
      }
    }

    public IEnumerable<IntDouble> agg_cnt_lineitem(int key, IEnumerable<IntIntDouble> values) {
      int cntValue = 0;
      int aggValue = 0;
      foreach (var value in values) {
        cntValue++;
        aggValue += value.second;
      }
      yield return new IntDouble(key, aggValue / cntValue * 0.2);
    }

    public string Usage {get {return "";} }

    public void Execute(string[] args) {
      using (var computation = Naiad.NewComputation.FromArgs(ref args)) {
        var part_input = new BatchedDataSource<string>();
        var lineitem_input = new BatchedDataSource<string>();
        var part = computation.NewInput(part_input).SelectMany(line => read_part(line));
        var lineitem = computation.NewInput(lineitem_input).SelectMany(line => read_lineitem(line));

        var agg_lin = lineitem.IntSum(row => row.first,
                                      row => row.second);
        var cnt_lin = lineitem.IntSum(row => row.first,
                                      row => 1);
        var avg_lineitem = agg_lin.Join(cnt_lin,
                             agg_row => agg_row.First,
                                        cnt_row => cnt_row.First,
                                        (agg_row, cnt_row) => new IntDouble(agg_row.First, agg_row.Second / cnt_row.Second * 0.2));

//          var avg_lineitem = lineitem.GroupBy(row => row.first,
//                           (key, selected) => agg_cnt_lineitem(key, selected));

        var linepart = lineitem.Join(part,
                         (IntIntDouble left) => left.first,
                         (IntStringString right) => right.first,
                         (IntIntDouble left, IntStringString right) => new IntIntDoubleStringString(left.first, left.second, left.third , right.second, right.third));

        var lineavg = linepart.Join(avg_lineitem,
                         (IntIntDoubleStringString left) => left.first,
                         (IntDouble right) => right.first,
                         (IntIntDoubleStringString left, IntDouble right) => new IntIntDoubleStringStringDouble(left.first, left.second, left.third, left.fourth, left.fifth , right.second));
        var avg_yearly = lineavg.Where(row => ((row.fourth == "Brand#13") && ((row.fifth == "MEDBAG") && (row.second > row.sixth)))).Select(row => row.third);

        int minThreadId = computation.Configuration.ProcessID * computation.Configuration.WorkerCount;
        StreamWriter[] file_avg_yearly = new StreamWriter[computation.Configuration.WorkerCount];
        for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
          int j = minThreadId + i;
          file_avg_yearly[i] = new StreamWriter("/tmp/home/icg27/avg_yearly/avg_yearly" + j + ".out");
        }
        avg_yearly.Subscribe((i, l) => { foreach (var element in l) file_avg_yearly[i - minThreadId].WriteLine(element); });

        computation.Activate();
        part_input.OnCompleted("/tmp/home/icg27/part/part" + computation.Configuration.ProcessID + ".in");
        lineitem_input.OnCompleted("/tmp/home/icg27/lineitem/lineitem" + computation.Configuration.ProcessID + ".in");
        computation.Join();
        for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
          file_avg_yearly[i].Close();
        }
      }
    }

    public string Help {
      get {
        return "TPCH";
      }
    }

  }

}
