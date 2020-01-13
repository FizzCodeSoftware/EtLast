namespace FizzCode.EtLast
{
    public delegate void ContextOnRowValueChangedDelegate(IRow row, string column, object previousValue, object newValue, IProcess process, IBaseOperation operation);
}