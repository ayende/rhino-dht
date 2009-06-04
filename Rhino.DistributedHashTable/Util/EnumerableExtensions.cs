using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Rhino.DistributedHashTable.Util
{
	public static class EnumerableExtensions
	{
		public static IEnumerable<T> Apply<T>(this IEnumerable<T> self, Action<T> action)
		{
			foreach (var item in self)
			{
				action(item);
				yield return item;
			}
		}

		public static void MoveTo<T>(this ICollection<T> self, ICollection<T> other, Func<T, bool> predicate)
		{
			var array = self.Where(predicate).ToArray();
			foreach (var item in array)
			{
				self.Remove(item);
				other.Add(item);
			}
		}

		public static void RemoveAll<T>(this ICollection<T> self, Func<T, bool> predicate)
		{
			var array = self.Where(predicate).ToArray();
			foreach (var item in array)
			{
				self.Remove(item);
			}
		}

		public static IEnumerable<T[]> Page<T>(this IEnumerable<T> self, int pageSize)
		{
			int pageNum = 0;
			while (true)
			{
				var page = self
					.Skip(pageNum * pageSize)
					.Take(pageSize)
					.ToArray();

				if (page.Length == 0)
					yield break;

				yield return page;
				pageNum += 1;
			}
		}


		public static IEnumerable<T> Append<T>(this IEnumerable<T> self, T item)
		{
			foreach (var i in self)
			{
				yield return i;
			}
			yield return item;
		}

		public static bool Empty(this IEnumerable self)
		{
			foreach (var _ in self)
			{
				return false;
			}
			return true;
		}

		public static void Consume(this IEnumerable self)
		{
			foreach (var _ in self)
			{
			}
		}
	}
}