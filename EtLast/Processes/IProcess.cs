namespace FizzCode.EtLast
{
    using System.ComponentModel;

    public interface IProcess
    {
        [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
        ProcessInvocationInfo InvocationInfo { get; set; }

        IEtlContext Context { get; }
        ITopic Topic { get; }
        string Name { get; }

        ProcessKind Kind { get; }
    }
}