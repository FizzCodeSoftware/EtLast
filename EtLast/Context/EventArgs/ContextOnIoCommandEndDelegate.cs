namespace FizzCode.EtLast
{
    using System;

    public delegate void ContextOnIoCommandEndDelegate(IProcess proces, int uid, int affectedDataCount, Exception ex);
}