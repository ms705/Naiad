using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
using Microsoft.Research.Naiad.Input;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace Microsoft.Research.Naiad.Examples.DifferentialDataflow {

  public class Netflix : Example {

    int movieSelect = 1970;

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

      public IntTriple(int x, int y, int z)
      {
        first = x; second = y; third = z;
      }
    }

    public struct Movie : IEquatable<Movie> {
      public int id;
      public int year;
      public string title;

      public bool Equals(Movie that) {
        return id == that.id && year == that.year && title.Equals(that.title);
      }

      public int CompareTo(Movie that) {
        if (id != that.id)
          return id - that.id;
        if (year != that.year)
          return year - that.year;
        return title.CompareTo(that.title);
      }

      public override int GetHashCode() {
        return 31 * id + 1234347 * year + 4311 * title.GetHashCode();
      }

      public override string ToString() {
        return String.Format("{0} {1} {2}", id, year, title);
      }

      public Movie(int x, int y, string z) {
        id = x; year = y; title = z;
      }

    }

    public struct Rating : IEquatable<Rating> {
      public int movId;
      public int userId;
      public int rating;
      public string date;

      public bool Equals(Rating that) {
        return movId == that.movId && userId == that.userId &&
          rating == that.rating && date.Equals(that.date);
      }

      public int CompareTo(Rating that) {
        if (movId != that.movId)
          return movId - that.movId;
        if (userId != that.userId)
          return userId - that.userId;
        if (rating != that.rating)
          return rating - that.rating;
        return date.CompareTo(that.date);
      }

      public override int GetHashCode() {
        return 31 * movId + 1234347 * userId + 4311 * rating + 12315 *
          date.GetHashCode();
      }

      public override string ToString() {
        return String.Format("{0} {1} {2} {3}", movId, userId, rating,
                             date);
      }

      public Rating(int x, int y, int z, string w) {
        movId = x;
        userId = y;
        rating = z;
        date = w;
      }

    }

    public IEnumerable<IntTriple> SumRatings(IntPair movIds,
                                             IEnumerable<int> ratings) {
      int allRatings = 0;
      foreach (var rating in ratings)
        allRatings += rating;
      yield return new IntTriple(movIds.s, movIds.t, allRatings);
    }

    public IEnumerable<IntPair> MaxRating(int id, IEnumerable<int> ratings) {
      int maxRating = 0;
      foreach (var rating in ratings)
        maxRating = Math.Max(maxRating, rating);
      yield return new IntPair(id, maxRating);
    }

    public void Execute(string[] args) {
      using (var computation = NewComputation.FromArgs(ref args)) {
        var movies = computation.NewInputCollection<Movie>();
        var ratings = computation.NewInputCollection<Rating>();

        var ratingSel = movies.Where((Movie x) => x.year < 1970)
          .Join(ratings,
                (Movie movie) => movie.id,
                (Rating rating) => rating.movId,
                (Movie movie, Rating rating) =>
                new IntTriple(rating.movId, rating.userId, rating.rating));

        var ratingJoin = ratingSel.Join(ratingSel,
                                        (IntTriple r1) => r1.second,
                                        (IntTriple r2) => r2.second,
                                        (IntTriple r1, IntTriple r2) =>
                                        new IntTriple(r1.first, // movId
                                                      r1.third * r2.third, // rating * rating
                                                      r2.first)); // movId

        var intResult = ratingJoin.GroupBy(row => new IntPair(row.first, row.third),
                                           row => row.second, // Select rating
                                           (movIds, grouped_ratings) =>
                                           SumRatings(movIds, grouped_ratings));

        var matmultfinalPrj = intResult.Join(ratingSel,
                       (IntTriple intRes) => intRes.first, // movId
                       (IntTriple ratSel) => ratSel.first, // movId
                       (IntTriple intRes, IntTriple ratSel) =>
                       new IntTriple(intRes.first,
                                     intRes.third * ratSel.third,
                                     ratSel.second));

        var predicted = matmultfinalPrj.GroupBy(row => new IntPair(row.first, row.third),
                                                row => row.second, // Select rating
                                                (movIds, grouped_ratings) =>
                                                SumRatings(movIds, grouped_ratings));

        var prediction = predicted.GroupBy(row => row.second,
                                           row => row.third,
                                           (id, movId) => MaxRating(id, movId))
          .Subscribe(l => { foreach (var element in l) Console.WriteLine(element); });

        computation.Activate();
        var inputFile = args[1];
        if (computation.Configuration.ProcessID == 0) {
          StreamReader reader = File.OpenText(inputFile);

          for (var i = 0; !reader.EndOfStream; i++) {
            var elements = reader.ReadLine().Split(' ');
            if (elements.Length == 3) {
              movies.OnNext(new Movie(Convert.ToInt32(elements[0]),
                                      Convert.ToInt32(elements[1]),
                                      elements[2]));
              ratings.OnNext();
              computation.Sync(i);
            }
            if (elements.Length == 4) {
              movies.OnNext();
              ratings.OnNext(new Rating(Convert.ToInt32(elements[0]),
                                        Convert.ToInt32(elements[1]),
                                        Convert.ToInt32(elements[2]),
                                        elements[3]));
              computation.Sync(i);
            }
          }
        }

        movies.OnCompleted();
        ratings.OnCompleted();

        computation.Join();
      }
    }

    public string Usage { get { return ""; } }

    public string Help {
      get {
        return "Netflix <input file>";
      }
    }
  }

}