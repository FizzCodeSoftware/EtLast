using System.Runtime.InteropServices;

namespace FizzCode.EtLast.Diagnostics.Windows;

public class ToolStripTextBoxWithPlaceHolderText : ToolStripTextBox
{
    private string _placeHolder;

    public ToolStripTextBoxWithPlaceHolderText()
    {
        Control.HandleCreated += (s, e) =>
        {
            if (!string.IsNullOrEmpty(_placeHolder))
                SetPlaceHolderText();
        };
    }

    public string PlaceHolderText
    {
        get => _placeHolder;
        set
        {
            _placeHolder = value;
            SetPlaceHolderText();
        }
    }

    private void SetPlaceHolderText()
    {
        const int EM_SETCUEBANNER = 0x1501;
        _ = SendMessage(Control.Handle, EM_SETCUEBANNER, 0, _placeHolder);
    }

    [DllImport("user32.dll", CharSet = CharSet.Unicode)]
    private static extern int SendMessage(IntPtr hWnd, int msg, int wParam, string lParam);
}
