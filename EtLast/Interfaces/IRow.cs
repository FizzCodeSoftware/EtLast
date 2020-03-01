namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IRow : IValueCollection
    {
        IEtlContext Context { get; }
        int Uid { get; }

        IProcess CreatorProcess { get; }
        IProcess CurrentProcess { get; set; }

        void Init(IEtlContext context, IProcess creatorProcess, int uid, IEnumerable<KeyValuePair<string, object>> initialValues); // called right after creation

        public void SetStagedValue(string column, object newValue);
        void ApplyStaging();
        bool HasStaging { get; }
    }
}