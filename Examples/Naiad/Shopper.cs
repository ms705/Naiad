using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using System.Text;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Frameworks.Reduction;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
using Microsoft.Research.Naiad.Input;

namespace Microsoft.Research.Naiad.Examples.Shopper {

  public class Shopper : Example {

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

    public struct IntIntIntInt : IEquatable<IntIntIntInt> {
      public int first;
      public int second;
      public int third;
      public int fourth;

      public bool Equals(IntIntIntInt that) {
        return first == that.first && second == that.second && third == that.third &&
          fourth == that.fourth;
      }

      public int CompareTo(IntIntIntIntInt that) {
        if (this.first != that.first)
          return this.first - that.first;

        if (this.second != that.second)
          return this.second - that.second;

        if (this.third != that.third)
          return this.third - that.third;

        return this.fourth - that.fourth;
      }

      // Embarassing hashcodes
      public override int GetHashCode() {
        return 31 * first + 1234347 * second + 4311 * third + 12315 * fourth;
      }

      public override string ToString() {
        return String.Format("{0} {1} {2} {3}", first, second, third, fourth);
      }

      public IntIntIntInt(int x, int y, int z, int w) {
        first = x; second = y; third = z; fourth = w;
      }
    }

    public struct IntIntIntIntInt : IEquatable<IntIntIntIntInt> {
      public int first;
      public int second;
      public int third;
      public int fourth;
      public int fifth;

      public bool Equals(IntIntIntIntInt that) {
        return first == that.first && second == that.second && third == that.third &&
          fourth == that.fourth && fifth == that.fifth;
      }

      public int CompareTo(IntIntIntIntInt that) {
        if (this.first != that.first)
          return this.first - that.first;

        if (this.second != that.second)
          return this.second - that.second;

        if (this.third != that.third)
          return this.third - that.third;

        if (this.fourth != that.fourth)
          return this.fourth - that.fourth;

        return this.fifth - that.fifth;
      }

      // Embarassing hashcodes
      public override int GetHashCode() {
        return 31 * first + 1234347 * second + 4311 * third + 12315 * fourth + 3 * fifth;
      }

      public override string ToString() {
        return String.Format("{0} {1} {2} {3} {4}", first, second, third, fourth, fifth);
      }

      public IntIntIntIntInt(int x, int y, int z, int w, int xx) {
        first = x; second = y; third = z; fourth = w; fifth = xx;
      }
    }

    public struct ReducerShop : IAddable<ReducerShop> {
      public int value;

      public ReducerShop(int value) {
        this.value = value;
      }

      public ReducerShop Add(ReducerShop other) {
        this.value += other.value;
        return this;
      }
    }

    public IEnumerable<IntIntIntInt> readShopLogs(string filename) {
      StreamReader reader = File.OpenText(filename);
      for (; !reader.EndOfStream; ) {
        var elements = reader.ReadLine().Split(' ');
        yield return new IntIntIntInt(Convert.ToInt32(elements[0]),
                                      Convert.ToInt32(elements[1]),
                                      Convert.ToInt32(elements[2]),
                                      Convert.ToInt32(elements[3]));
      }
    }

    public IEnumerable<IntIntIntInt> generateShopLogs(string filename) {
      int numUsers = 100000000;
      int numCountries = 50;
      int numProducts = 10000;
      Random random = new Random();
      for (int curUser = 0; curUser < numUsers; curUser++) {
        int numProductsBought = random.Next(1, 30);
        for (int curProd = 0; curProd < numProductsBought; curProd++)
        {
          // int productId = random.Next(0, numProducts);
          // int price = random.Next(1, 300);
          // if (curUser % 2 == 0) {
          //   yield return new IntIntIntInt(curUser, 1, productId, price);
          // } else {
          //   int countryId = random.Next(2, numCountries);
          //   yield return new IntIntIntInt(curUser, countryId, productId, price);
          // }
          yield return new IntIntIntInt(curUser, curUser % 2, 1, 100);
        }
      }
    }

    public IEnumerable<IntInt> Sum(int id, IEnumerable<IntIntIntInt> logs) {
      int aggVal = 0;
      foreach (var log in logs)
        aggVal += log.fourth;
      yield return new IntInt(id, aggVal);
    }

    public string Usage {get {return "shopping_file";} }

    public void Execute(string[] args) {
      using (var computation = Naiad.NewComputation.FromArgs(ref args)) {
        var shopLogs = new BatchedDataSource<string>();
        var topSpenders = computation.NewInput(shopLogs)
          .SelectMany(line => generateShopLogs(line))
          .Where(row => row.second == 1)
          .GenericAggregator(row => row.first,
                             row => new ReducerShop(row.fourth))
          .Select(row => new IntInt(row.First, row.Second.value))
          // .GroupBy(row => row.first,
          //          (id, grouped) => Sum(id, grouped))
          .Where(row => row.second > 12000);

        // Open output files.
        int minThreadId = computation.Configuration.ProcessID *
          computation.Configuration.WorkerCount;
        StreamWriter[] file_out = new StreamWriter[computation.Configuration.WorkerCount];
        for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
          int j = minThreadId + i;
          file_out[i] = new StreamWriter(args[2] + j + ".out");
        }

        topSpenders.Subscribe((i, l) => {
            foreach (var element in l)
              file_out[i - minThreadId].WriteLine(element); });

        computation.Activate();
        shopLogs.OnCompleted(args[1]);
        computation.Join();
        // Close output files.
        for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
          file_out[i].Close();
        }
      }
    }
    public string Help {
      get {
        return "Shopper <shop_logs> <output>";
      }
    }
  }

}