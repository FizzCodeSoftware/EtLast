namespace FizzCode.EtLast
{
    public interface IEditableRow : IReadOnlyRow
    {
        void SetValue(string column, object newValue);
    }
}