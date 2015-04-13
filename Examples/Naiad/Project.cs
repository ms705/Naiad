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

namespace Microsoft.Research.Naiad.Examples.Project {

  public class Project : Example
  {

    public IEnumerable<IntPair> read_rel(string filename)
    {
      StreamReader reader = File.OpenText(filename);
      for (; !reader.EndOfStream; ) {
        var elements = reader.ReadLine().Split(' ');
        yield return new IntPair(Convert.ToInt32(elements[0]),
                                 Convert.ToInt32(elements[1]));
      }
    }

    public string Usage {get {return "";} }

    public void Execute(string[] args) {
      using (var computation = Naiad.NewComputation.FromArgs(ref args)) {
        var input = new BatchedDataSource<string>();
        var rel = computation.NewInput(input).SelectMany(filename => read_rel(filename));

        var output = rel.Select(row => row.s);

        int minThreadId = computation.Configuration.ProcessID *
          computation.Configuration.WorkerCount;

        StreamWriter[] file_output = new StreamWriter[computation.Configuration.WorkerCount];
        for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
          int j = minThreadId + i;
          file_output[i] = new StreamWriter(args[2] + j + ".out");
        }
        output.Subscribe((i, l) => { foreach (var element in l) file_output[i - minThreadId].WriteLine(element); });
        computation.Activate();
        input.OnCompleted(args[1]);

        computation.Join();
        for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
          file_output[i].Close();
        }
      }
    }

    public string Help {
      get {
        return "Project <input> <output>";
      }
    }
  }

}
