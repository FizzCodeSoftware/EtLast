namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public class ContinuousAggregate
    {
        public SlimRow ResultRow { get; } = new SlimRow();
        public int RowsInGroup { get; set; }
        private Dictionary<string, object> _state;

        public object GetStateValue<T>(string uniqueName, T defaultValue)
        {
            if (_state == null)
                return defaultValue;

            if (_state.TryGetValue(uniqueName, out var v))
                return v;

            return defaultValue;
        }

        public void SetStateValue<T>(string uniqueName, T value)
        {
            if (_state == null)
                _state = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

            _state[uniqueName] = value;
        }
    }
}