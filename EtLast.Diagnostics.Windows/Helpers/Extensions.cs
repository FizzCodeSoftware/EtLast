namespace FizzCode.EtLast.Diagnostics.Windows;

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

        return string.Concat(text.AsSpan(0, length - 3), "...");
    }
}
