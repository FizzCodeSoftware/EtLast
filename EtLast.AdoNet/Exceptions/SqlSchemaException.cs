﻿namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlSchemaException(IProcess process, string message, Exception innerException)
    : EtlException(process, message, innerException)
{
}
