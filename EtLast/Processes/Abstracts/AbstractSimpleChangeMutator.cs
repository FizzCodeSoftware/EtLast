namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class AbstractSimpleChangeMutator : AbstractMutator
    {
        protected List<KeyValuePair<string, object>> Changes;

        protected AbstractSimpleChangeMutator(IEtlContext context)
            : base(context)
        {
        }

        protected override void StartMutator()
        {
            base.StartMutator();
            Changes = new List<KeyValuePair<string, object>>();
        }

        protected override void CloseMutator()
        {
            if (Changes != null)
            {
                Changes.Clear();
                Changes = null;
            }
        }
    }
}