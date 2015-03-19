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
using Microsoft.Research.Naiad.Input;

namespace Microsoft.Research.Naiad.Examples.Netflix
{

  public class Netflix : Example {

    public struct LongPair : IEquatable<LongPair>
    {
      public long s;
      public long t;

      public bool Equals(LongPair that)
      {
        return s == that.s && t == that.t;
      }

      public long CompareTo(LongPair that)
      {
        if (this.s != that.s)
          return this.s - that.s;

        return this.t - that.t;
      }

      public override int GetHashCode()
      {
        return 47 * s.GetHashCode() + 36425232 * t.GetHashCode();
      }

      public override string ToString()
      {
        return String.Format("{0} {1}", s, t);
      }

      public LongPair(long ss, long tt)
      {
        s = ss;
        t = tt;
      }

    }

    public struct LongTriple : IEquatable<LongTriple>
    {
      public long first;
      public long second;
      public long third;

      public bool Equals(LongTriple that)
      {
        return first == that.first && second == that.second && third == that.third;
      }

      public long CompareTo(LongTriple that)
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
        return first.GetHashCode() + 1234347 * second.GetHashCode() +
          4311 * third.GetHashCode();
      }

      public override string ToString()
      {
        return String.Format("{0} {1} {2}", first, second, third);
      }

      public LongTriple(long x, long y, long z)
      {
        first = x; second = y; third = z;
      }
    }

    public struct Movie : IEquatable<Movie>
    {
      public long id;
      public long year;
      public string title;

      public bool Equals(Movie that)
      {
        return id == that.id && year == that.year && title.Equals(that.title);
      }

      public long CompareTo(Movie that)
      {
        if (id != that.id)
          return id - that.id;
        if (year != that.year)
          return year - that.year;
        return title.CompareTo(that.title);
      }

      public override int GetHashCode()
      {
        return 31 * id.GetHashCode() + 1234347 * year.GetHashCode() +
          4311 * title.GetHashCode();
      }

      public override string ToString()
      {
        return String.Format("{0} {1} {2}", id, year, title);
      }

      public Movie(long x, long y, string z)
      {
        id = x; year = y; title = z;
      }

    }

    public struct Rating : IEquatable<Rating>
    {
      public long movId;
      public long userId;
      public long rating;

      public bool Equals(Rating that)
      {
        return movId == that.movId && userId == that.userId &&
          rating == that.rating;
      }

      public long CompareTo(Rating that)
      {
        if (movId != that.movId)
          return movId - that.movId;
        if (userId != that.userId)
          return userId - that.userId;
        return rating - that.rating;
      }

      public override int GetHashCode()
      {
        return 31 * movId.GetHashCode() + 1234347 * userId.GetHashCode() +
          4311 * rating.GetHashCode();
      }

      public override string ToString()
      {
        return String.Format("{0} {1} {2}", movId, userId, rating);
      }

      public Rating(long x, long y, long z)
      {
        movId = x;
        userId = y;
        rating = z;
      }

    }

    public IEnumerable<LongTriple> SumRatings(LongPair movIds,
                                             IEnumerable<LongTriple> ratings) {
      long allRatings = 0;
      foreach (var rating in ratings)
        allRatings += rating.second;
      yield return new LongTriple(movIds.s, movIds.t, allRatings);
    }

    public IEnumerable<LongPair> MaxRating(long id, IEnumerable<LongTriple> ratings) {
      long maxRating = 0;
      foreach (var rating in ratings)
        maxRating = Math.Max(maxRating, rating.third);
      yield return new LongPair(id, maxRating);
    }

    public IEnumerable<Rating> ReadRatings(String filename) {
      StreamReader reader = File.OpenText(filename);
      for (; !reader.EndOfStream; ) {
        var elements = reader.ReadLine().Split(' ');
        yield return new Rating(Convert.ToInt64(elements[0]),
                                Convert.ToInt64(elements[1]),
                                Convert.ToInt64(elements[2]));
      }
    }

    public IEnumerable<Movie> ReadMovies(String filename) {
      StreamReader reader = File.OpenText(filename);
      for (; !reader.EndOfStream; ) {
        var elements = reader.ReadLine().Split(' ');
        yield return new Movie(Convert.ToInt64(elements[0]),
                               Convert.ToInt64(elements[1]),
                               elements[2]);
      }
    }

    public string Usage {get {return "ratings_file movies_file year";} }

    public void Execute(string[] args) {
      using (var computation = NewComputation.FromArgs(ref args)) {
        var movies = new BatchedDataSource<string>();
        var ratings = new BatchedDataSource<string>();
        bool optimized = true;
        if (args.Length == 5) {
          optimized = Convert.ToBoolean(args[4]);
        }
        long year = Convert.ToInt64(args[3]);

        var movies_in = computation.NewInput(movies)
          .SelectMany(file => ReadMovies(file));
        var ratings_in = computation.NewInput(ratings)
          .SelectMany(file => ReadRatings(file));

        var ratingSel = movies_in.Where((Movie x) => x.year < year)
          .Join(ratings_in,
                (Movie movie) => movie.id,
                (Rating rating) => rating.movId,
                (Movie movie, Rating rating) =>
                new LongTriple(rating.movId, rating.userId, rating.rating));

        var ratingJoin = ratingSel.Join(ratingSel,
                                        (LongTriple r1) => r1.second,
                                        (LongTriple r2) => r2.second,
                                        (LongTriple r1, LongTriple r2) =>
                                        new LongTriple(r1.first, // movId
                                                       r1.third * r2.third, // rating * rating
                                                       r2.first)); // movId

        var intResult = ratingJoin.IntSum(row => new LongPair(row.first, row.third),
                                          row => row.second);

        var matmultfinalPrj = intResult.Join(ratingSel,
                                             intRes => intRes.First.s, // movId
                                             ratSel => ratSel.first, // movId
                                             (intRes, ratSel) =>
                                             new LongTriple(intRes.First.s,
                                                            intRes.Second * ratSel.third,
                                                            ratSel.second));

        var predicted = matmultfinalPrj.IntSum(row => new LongPair(row.first, row.third),
                                               row => row.second);

        if (optimized) {
          int minThreadId = computation.Configuration.ProcessID *
            computation.Configuration.WorkerCount;
          StreamWriter[] file_out = new StreamWriter[computation.Configuration.WorkerCount];
          for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
            int j = minThreadId + i;
            file_out[i] = new StreamWriter("/tmp/home/icg27/prediction/prediction" + j + ".out");
          }

          var prediction = predicted.Max(row => row.First.t,
                                         row => row.Second)
            .Subscribe((i, l) => { foreach (var element in l) file_out[i - minThreadId].WriteLine(element); });

          computation.Activate();
          movies.OnCompleted(args[2]);
          ratings.OnCompleted(args[1]);
          computation.Join();
          for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
            file_out[i].Close();
          }
        } else {
          var prediction = predicted.Max(row => row.First.t,
                                         row => row.Second);
          StreamWriter file_out = new StreamWriter("/tmp/home/icg27/prediction/prediction.out");
          if (computation.Configuration.ProcessID == 0) {
            prediction.Subscribe(l => { foreach (var element in l) file_out.WriteLine(element); });
            computation.Activate();
            movies.OnCompleted(args[2]);
            ratings.OnCompleted(args[1]);
          } else {
            computation.Activate();
            movies.OnCompleted();
            ratings.OnCompleted();
          }
          computation.Join();
          file_out.Close();
        }
      }
    }

    public string Help {
      get {
        return "Netflix <local path prefix>";
      }
    }

  }

}
