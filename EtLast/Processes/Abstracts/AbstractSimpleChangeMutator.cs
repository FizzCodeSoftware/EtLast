namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class AbstractSimpleChangeMutator : AbstractMutator
    {
        protected List<KeyValuePair<string, object>> Changes;

        protected AbstractSimpleChangeMutator(ITopic topic, string name)
            : base(topic, name)
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