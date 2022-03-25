namespace FizzCode.EtLast.Diagnostics.Windows;

using System;
using System.Windows.Forms;

public static class ToolTipSingleton
{
    private static readonly ToolTip _toolTip = new()
    {
        ShowAlways = true,
        AutoPopDelay = 5000,
        InitialDelay = 0,
        ReshowDelay = 500,
        IsBalloon = true,
    };

    public static void Show(object value, Control control, int x, int y)
    {
        x += 8;
        y += 8;

        var displayText = value switch
        {
            string text => text,
            Func<string> textFunc => textFunc.Invoke(),
            _ => null,
        };

        if (!string.IsNullOrEmpty(displayText))
            displayText = displayText.Trim();

        if (!string.IsNullOrEmpty(displayText))
        {
            _toolTip.Show(displayText, control, x, y);
        }
        else
        {
            Remove(control);
        }
    }

    public static void Remove(Control control)
    {
        _toolTip.SetToolTip(control, "");
    }
}
