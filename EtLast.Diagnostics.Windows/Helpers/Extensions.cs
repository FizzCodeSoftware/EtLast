namespace FizzCode.EtLast.Diagnostics.Windows;

using System.Collections;
using System.Collections.Generic;

public static class Extensions
{
    public static IEnumerable<T> ToEnumerable<T>(this IEnumerable enumerable)
    {
        foreach (T item in enumerable)
        {
            yield return item;
        }
    }

    public static string MaxLengthWithEllipsis(this string text, int length)
    {
        if (text.Length <= length)
            return text;

        return text.Substring(0, length - 3) + "...";
    }
}
