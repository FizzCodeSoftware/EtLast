namespace FizzCode.EtLast.Diagnostics.Windows;

#pragma warning disable CA2213 // Disposable fields should be disposed
public partial class MainForm : Form
{
    private readonly TabControl _sessionTabs;
    private readonly DiagnosticsStateManager _stateManager;
    private readonly Dictionary<string, SessionControl> _sessionTabManagers = new();
    private readonly Timer _timer;

    public MainForm()
    {
        InitializeComponent();

        Font = new Font(Font.FontFamily, 9, FontStyle.Regular);
        Text = "EtLast Diagnostics";

        _stateManager = new DiagnosticsStateManager("http://+:8642/");
        _stateManager.OnDiagSessionCreated += SessionCreated;

        _sessionTabs = new TabControl()
        {
            Dock = DockStyle.Fill,
            Parent = this,
            DrawMode = TabDrawMode.OwnerDrawFixed,
            SizeMode = TabSizeMode.Fixed,
            ItemSize = new Size(96, 24),
        };

        _sessionTabs.DrawItem += DrawSessionTabHeader;
        _sessionTabs.MouseDown += SessionTabs_MouseDown;

        _timer = new Timer()
        {
            Interval = 500,
            Enabled = false,
        };
        _timer.Tick += UpdateTimerTick;

        _stateManager.Start();
        _timer.Start();
    }

    private static Rectangle GetSessionTabCloseButtonRectangle(Rectangle tabRectable)
    {
        var height = 12;
        return new Rectangle(tabRectable.Right - 10 - height, tabRectable.Top + ((tabRectable.Height - height) / 2), height, height);
    }

    private void SessionTabs_MouseDown(object sender, MouseEventArgs e)
    {
        var control = sender as TabControl;
        for (var i = 0; i < control.TabPages.Count; i++)
        {
            var rect = GetSessionTabCloseButtonRectangle(control.GetTabRect(i));
            if (rect.Contains(e.Location))
            {
                if (MessageBox.Show("...you want to close this session?", "Are you sure...", MessageBoxButtons.YesNo, MessageBoxIcon.Question) == DialogResult.Yes)
                {
                    var page = control.TabPages[i];
                    var sessionId = page.Name;

                    control.TabPages.Remove(page);

                    if (_sessionTabManagers.TryGetValue(sessionId, out var sessionManager))
                    {
                        _sessionTabManagers.Remove(sessionId);
                        sessionManager.Close();
                    }

                    page.Dispose();
                    break;
                }
            }
        }
    }

    private readonly Font _sessionTabCloseFont = new("courier new", 10);

    private void DrawSessionTabHeader(object sender, DrawItemEventArgs e)
    {
        var control = sender as TabControl;
        var textRect = e.Bounds;
        var xRect = GetSessionTabCloseButtonRectangle(textRect);

        var xFont = _sessionTabCloseFont;
        var xSize = e.Graphics.MeasureString("x", xFont);
        //e.Graphics.FillEllipse(Brushes.Red, xRect);
        e.Graphics.DrawString("x", xFont, Brushes.Black, xRect.Left + ((xRect.Width - xSize.Width) / 2), xRect.Top + ((xRect.Height - xSize.Height) / 2));

        var textFont = e.Font;
        var textSize = e.Graphics.MeasureString(control.TabPages[e.Index].Text, textFont);
        e.Graphics.DrawString(control.TabPages[e.Index].Text, textFont, Brushes.Black, textRect.Left + 12, textRect.Top + ((textRect.Height - textSize.Height) / 2));
        e.DrawFocusRectangle();
    }

    private void UpdateTimerTick(object sender, EventArgs e)
    {
        _timer.Stop();
        _stateManager.ProcessEvents();
        _timer.Start();
    }

    private void SessionCreated(DiagSession session)
    {
        var sessionContainer = new TabPage(session.SessionId)
        {
            BorderStyle = BorderStyle.None,
            Name = session.SessionId,
        };

        var manager = new SessionControl(session, sessionContainer, _stateManager);
        _sessionTabManagers.Add(session.SessionId, manager);
        _sessionTabs.TabPages.Add(sessionContainer);
        _sessionTabs.SelectedTab = sessionContainer;
    }

    protected override void OnLoad(EventArgs e)
    {
        base.OnLoad(e);

        MaximizeOnSecondaryScreen();
    }

    private void MaximizeOnSecondaryScreen()
    {
        var screen = Screen.AllScreens.Length == 1 || true
            ? Screen.PrimaryScreen
            : Screen.AllScreens
                .Where(x => !x.Primary)
                .OrderByDescending(x => x.Bounds.Width)
                .First();

        SuspendLayout();
        Bounds = new Rectangle(screen.Bounds.Left, screen.Bounds.Top, Bounds.Width, Bounds.Height);
        WindowState = FormWindowState.Maximized;
        ResumeLayout();
    }
}
#pragma warning restore CA2213 // Disposable fields should be disposed