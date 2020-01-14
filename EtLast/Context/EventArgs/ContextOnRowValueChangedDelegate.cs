namespace FizzCode.EtLast
{
    public delegate void ContextOnRowValueChangedDelegate(IRow row, string column, object previousValue, object currentValue, IProcess process, IBaseOperation operation);
}