namespace FizzCode.EtLast.Diagnostics.Interface;

public class ExtendedBinaryWriter : BinaryWriter
{
    public ExtendedBinaryWriter(Stream output, Encoding encoding)
        : base(output, encoding)
    {
    }

    public void WriteNullable(string value)
    {
        Write(value != null);
        if (value != null)
            Write(value);
    }

    public void WriteNullable(int? value)
    {
        Write(value != null);
        if (value != null)
            Write7BitEncodedInt(value.Value);
    }

    public void WriteNullable(long? value)
    {
        Write(value != null);
        if (value != null)
            Write(value.Value);
    }

    public void WriteNullable7BitEncodedInt32(int? value)
    {
        Write(value != null);
        if (value != null)
            Write7BitEncodedInt(value.Value);
    }

    public void WriteNullable7BitEncodedInt64(long? value)
    {
        Write(value != null);
        if (value != null)
            Write7BitEncodedInt64(value.Value);
    }

    public void WriteObject(object value)
    {
        Write(value != null);

        if (value == null)
            return;

        if (value is EtlRowError rowErr)
        {
            Write((byte)ArgumentType._error);
            Write(rowErr.Message);
            return;
        }

        if (value is EtlRowRemovedValue)
        {
            Write((byte)ArgumentType._removed);
            return;
        }

        if (value is string sv)
        {
            Write((byte)ArgumentType._string);
            Write(sv);
            return;
        }

        if (value is bool bv)
        {
            Write((byte)ArgumentType._bool);
            Write(bv);
            return;
        }

        if (value is char cv)
        {
            Write((byte)ArgumentType._char);
            Write(cv);
            return;
        }

        if (value is sbyte sbytev)
        {
            Write((byte)ArgumentType._sbyte);
            Write(sbytev);
            return;
        }

        if (value is byte bytev)
        {
            Write((byte)ArgumentType._byte);
            Write(bytev);
            return;
        }

        if (value is short shortv)
        {
            Write((byte)ArgumentType._short);
            Write(shortv);
            return;
        }

        if (value is ushort ushortv)
        {
            Write((byte)ArgumentType._ushort);
            Write(ushortv);
            return;
        }

        if (value is int iv)
        {
            Write((byte)ArgumentType._int);
            Write7BitEncodedInt(iv);
            return;
        }

        if (value is uint uintv)
        {
            Write((byte)ArgumentType._uint);
            Write(uintv);
            return;
        }

        if (value is long lv)
        {
            Write((byte)ArgumentType._long);
            Write(lv);
            return;
        }

        if (value is ulong ulv)
        {
            Write((byte)ArgumentType._ulong);
            Write(ulv);
            return;
        }

        if (value is float fv)
        {
            Write((byte)ArgumentType._float);
            Write(fv);
            return;
        }

        if (value is double dv)
        {
            Write((byte)ArgumentType._double);
            Write(dv);
            return;
        }

        if (value is decimal decv)
        {
            Write((byte)ArgumentType._decimal);
            Write(decv);
            return;
        }

        if (value is TimeSpan ts)
        {
            Write((byte)ArgumentType._timespan);
            Write(ts.TotalMilliseconds);
            return;
        }

        if (value is DateTime dt)
        {
            Write((byte)ArgumentType._datetime);
            Write(dt.Ticks);
            return;
        }

        if (value is DateTimeOffset dto)
        {
            Write((byte)ArgumentType._datetimeoffset);
            Write(dto.Ticks);
            Write(dto.Offset.Ticks);
            return;
        }

        if (value is string[] strArr)
        {
            Write((byte)ArgumentType._stringArray);
            Write7BitEncodedInt(strArr.Length);
            foreach (var str in strArr)
            {
                WriteNullable(str);
            }

            return;
        }

        var valueType = value.GetType();
        if (valueType.IsClass)
        {
            Write((byte)ArgumentType._string);
            Write(valueType.Name);
            return;
        }

        Write((byte)ArgumentType._string);
        Write(value.ToString());
    }
}
