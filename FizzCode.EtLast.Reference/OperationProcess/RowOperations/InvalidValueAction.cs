namespace FizzCode.EtLast
{
    /// <summary>
    /// Controls the behavior when the operation fails on a row
    /// </summary>
    public enum InvalidValueAction
    {
        /// <summary>
        /// Keep the original value.
        /// </summary>
        Keep,

        /// <summary>
        /// Replace the original value by the specified value.
        /// </summary>
        SetSpecialValue,

        /// <summary>
        /// Stop processing the row and remove from the result.
        /// </summary>
        RemoveRow,

        /// <summary>
        /// Throw exception. 
        /// </summary>
        Throw,

        /// <summary>
        /// Wrap the value by an EtlError object.
        /// </summary>
        WrapError,
    }
}