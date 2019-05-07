﻿namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class OperationParameterNullException : InvalidOperationParameterException
    {
        public OperationParameterNullException(IBaseOperation operation, string parameterName)
            : base(operation, parameterName, null, "value cannot be null or empty")
        {
        }
    }
}