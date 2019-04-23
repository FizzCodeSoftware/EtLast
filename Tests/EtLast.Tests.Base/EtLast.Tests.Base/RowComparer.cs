namespace FizzCode.EtLast.Tests.Base
{
    using FizzCode.EtLast;

    public static class RowComparer
    {
        public static bool Equals(IRow row1, IRow row2)
        {
            if (row1 == row2)
                return true;

            if(row1 == null && row2 != null
                || row1 != null && row2 == null)
                return false;

            foreach(var kvp in row1.Values)
            {
                if (!kvp.Value.Equals(row2[kvp.Key]))
                    return false;
            }

            return true;
        }
    }
}
