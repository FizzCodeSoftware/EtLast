namespace FizzCode.EtLast
{
    public interface IValueCollection : IReadOnlyValueCollection
    {
        void SetValue(string column, object newValue);
    }
}