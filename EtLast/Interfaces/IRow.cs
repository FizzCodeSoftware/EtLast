namespace FizzCode.EtLast;

public interface IRow : IReadOnlyRow, ISlimRow
{
    new IProcess CurrentProcess { get; set; }

    void Init(IEtlContext context, IProcess creatorProcess, long uid, IEnumerable<KeyValuePair<string, object>> initialValues); // called right after creation

    void MergeWith(IEnumerable<KeyValuePair<string, object>> values);
}
