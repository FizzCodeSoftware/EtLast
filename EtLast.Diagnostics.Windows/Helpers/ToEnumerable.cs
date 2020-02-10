namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System.Collections;
    using System.Collections.Generic;

    public static class ToEnumerableHelpers
    {
        public static IEnumerable<T> ToEnumerable<T>(this IEnumerable enumerable)
        {
            foreach (T item in enumerable)
            {
                yield return item;
            }
        }
    }
}
