namespace FizzCode.EtLast.Diagnostics.Windows;

public partial class MainForm : Form
{
    private readonly TabControl ContextTabs;
    private readonly DiagnosticsStateManager _stateManager;
    private readonly Dictionary<long, ContextControl> _contextControls = [];
    private readonly Timer _timer;

    public MainForm()
    {
        InitializeComponent();

        Font = new Font(Font.FontFamily, 9, FontStyle.Regular);
        Text = "EtLast Diagnostics";

        _stateManager = new DiagnosticsStateManager("http://+:8642/");
        _stateManager.OnDiagContextCreated += ContextCreated;

        ContextTabs = new TabControl()
        {
            Dock = DockStyle.Fill,
            Parent = this,
            DrawMode = TabDrawMode.OwnerDrawFixed,
            SizeMode = TabSizeMode.Fixed,
            ItemSize = new Size(160, 24),
        };

        ContextTabs.DrawItem += DrawContextTabHeader;
        ContextTabs.MouseDown += ContextTabs_MouseDown;

        _timer = new Timer()
        {
            Interval = 500,
            Enabled = false,
        };
        _timer.Tick += UpdateTimerTick;

        _stateManager.Start();
        _timer.Start();
    }

    private static Rectangle GetCloseButtonRectangle(Rectangle tabRectable)
    {
        const int height = 12;
        return new Rectangle(tabRectable.Right - 10 - height, tabRectable.Top + ((tabRectable.Height - height) / 2), height, height);
    }

    private void ContextTabs_MouseDown(object sender, MouseEventArgs e)
    {
        var control = sender as TabControl;
        for (var i = 0; i < control.TabPages.Count; i++)
        {
            var rect = GetCloseButtonRectangle(control.GetTabRect(i));
            if (rect.Contains(e.Location))
            {
                if (MessageBox.Show("...you want to close this stream?", "Are you sure...", MessageBoxButtons.YesNo, MessageBoxIcon.Question) == DialogResult.Yes)
                {
                    var page = control.TabPages[i];
                    var contextId = long.Parse(page.Name);

                    control.TabPages.Remove(page);

                    if (_contextControls.TryGetValue(contextId, out var contextControl))
                    {
                        _contextControls.Remove(contextId);
                        contextControl.Close();
                    }

                    page.Dispose();
                    break;
                }
            }
        }
    }

    private void DrawContextTabHeader(object sender, DrawItemEventArgs e)
    {
        var control = sender as TabControl;
        var textRect = e.Bounds;
        var xRect = GetCloseButtonRectangle(textRect);

        var xFont = control.Font;
        var xSize = e.Graphics.MeasureString("x", xFont);
        e.Graphics.DrawString("x", xFont, Brushes.Black, xRect.Left + ((xRect.Width - xSize.Width) / 2), xRect.Top + ((xRect.Height - xSize.Height) / 2));

        var textFont = e.Font;
        var textSize = e.Graphics.MeasureString(control.TabPages[e.Index].Text, textFont);
        e.Graphics.DrawString(control.TabPages[e.Index].Text, textFont, Brushes.Black, textRect.Left + 12, textRect.Top + ((textRect.Height - textSize.Height) / 2));
    }

    private void UpdateTimerTick(object sender, EventArgs e)
    {
        _timer.Stop();
        _stateManager.ProcessEvents();
        _timer.Start();
    }

    private void ContextCreated(DiagContext context)
    {
        var container = new TabPage(context.Id.ToString("D", CultureInfo.InvariantCulture))
        {
            BorderStyle = BorderStyle.None,
            Name = context.Id.ToString("D", CultureInfo.InvariantCulture),
        };

        var control = new ContextControl(context, container);
        _contextControls.Add(context.Id, control);
        ContextTabs.TabPages.Add(container);
        ContextTabs.SelectedTab = container;
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