namespace FizzCode.EtLast
{
    using System;

    public abstract class AbstractDeferredCrossOperation : AbstractDeferredRowOperation
    {
        public Func<IRow[], IProcess> RightProcessCreator { get; set; }

        public override void Prepare()
        {
            base.Prepare();
            if (RightProcessCreator == null)
                throw new OperationParameterNullException(this, nameof(RightProcessCreator));
        }
    }
}