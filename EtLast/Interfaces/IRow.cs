namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IRow : IReadOnlyRow, ISlimRow
    {
        new IProcess CurrentProcess { get; set; }

        void Init(IEtlContext context, IProcess creatorProcess, int uid, IEnumerable<KeyValuePair<string, object>> initialValues); // called right after creation

        void SetStagedValue(string column, object newValue);
        void ApplyStaging();
        bool HasStaging { get; }
    }
}