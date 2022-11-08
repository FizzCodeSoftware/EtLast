namespace FizzCode.EtLast;

public class TextReaderStringBuilder
{
    private char[] _buffer = new char[8];
    public int Start { get; private set; }
    public int Length { get; private set; }

    public void Append(char ch)
    {
        if (Length >= _buffer.Length)
        {
            Array.Resize(ref _buffer, _buffer.Length + 8);
        }

        _buffer[Length++] = ch;
    }

    public void Clear()
    {
        Length = 0;
        Start = 0;
    }

    public void RemoveSurroundingDoubleQuotes()
    {
        if (Length < 2)
            return;

        if (Length == 2)
        {
            if (_buffer[0] == '\"' && _buffer[1] == '\"')
            {
                Start = 1;
                Length = 0;
            }
        }
        else
        {
            if (_buffer[0] == '\"' && _buffer[Length - 1] == '\"')
            {
                Start = 1;
                Length -= 2;
            }
        }
    }

    public string GetContentAsString()
    {
        return new string(_buffer, Start, Length);
    }

    public ReadOnlySpan<char> GetContentAsSpan()
    {
        return new ReadOnlySpan<char>(_buffer, Start, Length);
    }
}