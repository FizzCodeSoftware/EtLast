namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.IO;

    public static class DiagnosticsBinaryHelpers
    {
        public static void WriteNullable(this BinaryWriter writer, string value)
        {
            writer.Write(value != null);
            if (value != null)
                writer.Write(value);
        }

        public static void WriteNullable(this BinaryWriter writer, int? value)
        {
            writer.Write(value != null);
            if (value != null)
                writer.Write(value.Value);
        }

        public static string ReadNullableString(this BinaryReader reader)
        {
            if (!reader.ReadBoolean())
                return null;

            return reader.ReadString();
        }

        public static int? ReadNullableInt32(this BinaryReader reader)
        {
            if (!reader.ReadBoolean())
                return null;

            return reader.ReadInt32();
        }

        public static void WriteObject(this BinaryWriter writer, object value)
        {
            writer.Write(value != null);

            if (value == null)
                return;

            if (value is EtlRowError rowErr)
            {
                writer.Write((byte)ArgumentType._error);
                writer.Write(rowErr.Message);
                return;
            }

            if (value is string sv)
            {
                writer.Write((byte)ArgumentType._string);
                writer.Write(sv);
                return;
            }

            if (value is bool bv)
            {
                writer.Write((byte)ArgumentType._bool);
                writer.Write(bv);
                return;
            }

            if (value is char cv)
            {
                writer.Write((byte)ArgumentType._char);
                writer.Write(cv);
                return;
            }

            if (value is sbyte sbytev)
            {
                writer.Write((byte)ArgumentType._sbyte);
                writer.Write(sbytev);
                return;
            }

            if (value is byte bytev)
            {
                writer.Write((byte)ArgumentType._byte);
                writer.Write(bytev);
                return;
            }

            if (value is short shortv)
            {
                writer.Write((byte)ArgumentType._short);
                writer.Write(shortv);
                return;
            }

            if (value is ushort ushortv)
            {
                writer.Write((byte)ArgumentType._ushort);
                writer.Write(ushortv);
                return;
            }

            if (value is int iv)
            {
                writer.Write((byte)ArgumentType._int);
                writer.Write(iv);
                return;
            }

            if (value is uint uintv)
            {
                writer.Write((byte)ArgumentType._uint);
                writer.Write(uintv);
                return;
            }

            if (value is long lv)
            {
                writer.Write((byte)ArgumentType._long);
                writer.Write(lv);
                return;
            }

            if (value is ulong ulv)
            {
                writer.Write((byte)ArgumentType._ulong);
                writer.Write(ulv);
                return;
            }

            if (value is float fv)
            {
                writer.Write((byte)ArgumentType._float);
                writer.Write(fv);
                return;
            }

            if (value is double dv)
            {
                writer.Write((byte)ArgumentType._double);
                writer.Write(dv);
                return;
            }

            if (value is decimal decv)
            {
                writer.Write((byte)ArgumentType._decimal);
                writer.Write(decv);
                return;
            }

            if (value is TimeSpan ts)
            {
                writer.Write((byte)ArgumentType._timespan);
                writer.Write(ts.TotalMilliseconds);
                return;
            }

            if (value is DateTime dt)
            {
                writer.Write((byte)ArgumentType._datetime);
                writer.Write(dt.Ticks);
                return;
            }

            if (value is DateTimeOffset dto)
            {
                writer.Write((byte)ArgumentType._datetimeoffset);
                writer.Write(dto.Ticks);
                writer.Write(dto.Offset.Ticks);
                return;
            }

            if (value is string[] strArr)
            {
                writer.Write((byte)ArgumentType._stringArray);
                writer.Write((ushort)strArr.Length);
                foreach (var str in strArr)
                {
                    writer.WriteNullable(str);
                }

                return;
            }

            var valueType = value.GetType();
            if (valueType.IsClass)
            {
                writer.Write((byte)ArgumentType._string);
                writer.Write(valueType.Name);
                return;
            }

            writer.Write((byte)ArgumentType._string);
            writer.Write(value.ToString());
        }

        public static object ReadObject(this BinaryReader reader)
        {
            var hasValue = reader.ReadBoolean();
            if (!hasValue)
                return null;

            var type = (ArgumentType)reader.ReadByte();

            switch (type)
            {
                case ArgumentType._error:
                    return new EtlRowError(reader.ReadString());
                case ArgumentType._string:
                    return reader.ReadString();
                case ArgumentType._stringArray:
                    var count = reader.ReadUInt16();
                    var strArr = new string[count];
                    for (var i = 0; i < count; i++)
                    {
                        strArr[i] = reader.ReadNullableString();
                    }
                    return strArr;
                case ArgumentType._bool:
                    return reader.ReadBoolean();
                case ArgumentType._char:
                    return reader.ReadChar();
                case ArgumentType._sbyte:
                    return reader.ReadSByte();
                case ArgumentType._byte:
                    return reader.ReadByte();
                case ArgumentType._short:
                    return reader.ReadInt16();
                case ArgumentType._ushort:
                    return reader.ReadUInt16();
                case ArgumentType._int:
                    return reader.ReadInt32();
                case ArgumentType._uint:
                    return reader.ReadUInt32();
                case ArgumentType._long:
                    return reader.ReadInt64();
                case ArgumentType._ulong:
                    return reader.ReadUInt64();
                case ArgumentType._float:
                    return reader.ReadSingle();
                case ArgumentType._double:
                    return reader.ReadDouble();
                case ArgumentType._decimal:
                    return reader.ReadDecimal();
                case ArgumentType._datetime:
                    return new DateTime(reader.ReadInt64());
                case ArgumentType._datetimeoffset:
                    var ticks = reader.ReadInt64();
                    var offsetTicks = reader.ReadInt64();
                    return new DateTimeOffset(ticks, new TimeSpan(offsetTicks));
                case ArgumentType._timespan:
                    return TimeSpan.FromMilliseconds(reader.ReadDouble());
                default:
                    throw new NotSupportedException(nameof(ArgumentType) + "." + type.ToString());
            }
        }
    }
}
