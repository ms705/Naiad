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

namespace Microsoft.Research.Naiad.Examples.ReadTest
{

  public class ReadTest : Example {

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

    public string Usage {get {return "";} }

    public void Execute(string[] args) {
      using (var computation = Naiad.NewComputation.FromArgs(ref args)) {
        var part_input = new BatchedDataSource<string>();
        var lineitem_input = new BatchedDataSource<string>();
        bool multi_machine = true;
        if (args.Length == 2) {
          multi_machine = Convert.ToBoolean(args[1]);
        }
        var part = computation.NewInput(part_input).SelectMany(line => read_part(line));
        var lineitem = computation.NewInput(lineitem_input).SelectMany(line => read_lineitem(line));

        if (multi_machine) {
          part.Subscribe((i, l) => { });
          lineitem.Subscribe((i, l) => { });
          computation.Activate();
          part_input.OnCompleted("/tmp/part/part" +
                                 computation.Configuration.ProcessID + ".in");
          lineitem_input.OnCompleted("/tmp/lineitem/lineitem" +
                                     computation.Configuration.ProcessID + ".in");
        } else {
          part.Subscribe(l => { });
          lineitem.Subscribe(l => { });
          computation.Activate();
          if (computation.Configuration.ProcessID == 0) {
            part_input.OnCompleted("/tmp/part/part.in");
            lineitem_input.OnCompleted("/tmp/lineitem/lineitem.in");
          } else {
            part_input.OnNext();
            lineitem_input.OnNext();
            part_input.OnCompleted();
            lineitem_input.OnCompleted();
          }
        }
        computation.Join();
      }
    }

    public string Help {
      get {
        return "ReadTest [multi_machine]";
      }
    }
  }

}