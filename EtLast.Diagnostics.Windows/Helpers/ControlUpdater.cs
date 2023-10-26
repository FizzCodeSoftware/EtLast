namespace FizzCode.EtLast.Diagnostics.Windows;

internal class ControlUpdater<TItem>
{
    public DiagContext Context { get; }
    public Control Container { get; }
    private readonly int _interval;
    public ObjectListView ListView { get; }
    public Func<TItem, bool> ItemFilter { get; set; }
    public TextBox SearchBox { get; private set; }
    public bool AutoUpdateUntilContextLoaded { get; set; }
    public bool ContainsRows { get; set; }
    public EventHandler<EventArgs> RefreshStarted { get; set; }
    public EventHandler<EventArgs> RefreshFinished { get; set; }

    private readonly Timer _timer;
    private readonly List<TItem> _allItems = new();
    private bool _newItem;

    public ControlUpdater(DiagContext context, Control container, int interval = 1000, int firstInterval = 100)
    {
        Context = context;
        Container = container;
        _interval = interval;

        _timer = new Timer()
        {
            Interval = firstInterval,
            Enabled = false,
        };

        ListView = CreateListView(container);

        _timer.Tick += Timer_Tick;
    }

    public static ObjectListView CreateListView(Control container)
    {
        var listView = new FastObjectListView()
        {
            Parent = container,
            BorderStyle = BorderStyle.FixedSingle,
            ShowItemToolTips = true,
            ShowGroups = false,
            UseFiltering = true,
            ShowCommandMenuOnRightClick = true,
            SelectColumnsOnRightClickBehaviour = ObjectListView.ColumnSelectBehaviour.None,
            ShowFilterMenuOnRightClick = true,
            FullRowSelect = true,
            UseAlternatingBackColors = true,
            HeaderUsesThemes = true,
            GridLines = true,
            AlternateRowBackColor = Color.FromArgb(240, 240, 240),
            UseFilterIndicator = true,
            FilterMenuBuildStrategy = new DiagnosticsFilterMenuBuilder()
            {
                MaxObjectsToConsider = int.MaxValue,
            },
            MultiSelect = false,
            HideSelection = false,
            /*UseHotItem = true,
            HotItemStyle = new HotItemStyle()
            {
                Decoration = new RowBorderDecoration()
                {
                    BorderPen = new Pen(Color.DarkBlue, 1),
                    BoundsPadding = new Size(1, 1),
                    CornerRounding = 8,
                },
            },*/
        };

        listView.CellToolTip.IsBalloon = true;

        return listView;
    }

    public void CreateSearchBox(int x, int y, int w = 150, int h = 20)
    {
        SearchBox = new TextBox()
        {
            Parent = Container,
            Bounds = new Rectangle(x, y, w, h),
        };

        SearchBox.TextChanged += (sender, e) =>
        {
            ListView.AdditionalFilter = !string.IsNullOrEmpty(SearchBox.Text)
                ? TextMatchFilter.Contains(ListView, SearchBox.Text)
                : null;
        };
    }

    public void AddItem(TItem item)
    {
        _allItems.Add(item);
        _newItem = true;
    }

    public void RefreshItems(bool resizeColumns)
    {
        RefreshStarted?.Invoke(this, EventArgs.Empty);

        _newItem = false;

        IEnumerable<TItem> query = _allItems;
        if (ItemFilter != null)
        {
            query = query.Where(ItemFilter);
        }

        if (resizeColumns)
        {
            ListView.BeginUpdate();
            try
            {
                ListView.SetObjects(query);

                ResizeListView(ListView);
            }
            finally
            {
                ListView.EndUpdate();
            }
        }
        else
        {
            ListView.SetObjects(query);
        }

        RefreshFinished?.Invoke(this, EventArgs.Empty);
    }

    private void ResizeListView(ObjectListView listView)
    {
        if (listView.Items.Count == 0)
            return;

        foreach (OLVColumn col in listView.Columns)
        {
            col.MinimumWidth = 0;
            col.AutoResize(ContainsRows ? ColumnHeaderAutoResizeStyle.HeaderSize : ColumnHeaderAutoResizeStyle.ColumnContent);
        }

        foreach (OLVColumn col in listView.Columns)
        {
            col.Width += 5;
        }
    }

    public static void ResizeListViewWithRows(ObjectListView listView)
    {
        foreach (OLVColumn col in listView.Columns)
        {
            col.MinimumWidth = 0;
            col.AutoResize(ColumnHeaderAutoResizeStyle.HeaderSize);
        }

        foreach (OLVColumn col in listView.Columns)
        {
            col.Width += 20;
        }
    }

    private void Timer_Tick(object sender, EventArgs e)
    {
        if (ListView?.Visible == true)
        {
            var refresh = _newItem;
            if (refresh)
            {
                RefreshItems(true);
            }
            else if (AutoUpdateUntilContextLoaded)
            {
                if (Context?.FullyLoaded == true)
                {
                    AutoUpdateUntilContextLoaded = false;
                }

                ListView.Invalidate();
            }
        }

        if (_interval != -1)
        {
            if (_timer.Interval != _interval)
                _timer.Interval = _interval;
        }
        else
        {
            _timer.Stop();
        }
    }

    public void Start()
    {
        _timer.Start();
    }
}
