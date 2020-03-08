namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Drawing;
    using System.Linq;
    using System.Windows.Forms;
    using BrightIdeasSoftware;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class ControlUpdater<TItem>
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public AbstractDiagContext Context { get; }
        public Control Container { get; }
        public int Interval { get => _timer.Interval; set => _timer.Interval = value; }
        public ObjectListView ListView { get; set; }
        public Func<TItem, bool> ItemFilter { get; set; }
        public TextBox SearchBox { get; private set; }
        public bool AutoUpdateUntilContextLoaded { get; set; }

        private readonly Timer _timer;
        private readonly List<TItem> _allItems = new List<TItem>();
        private bool _newItem;

        public ControlUpdater(AbstractDiagContext context, Control container, int interval = 1000)
        {
            Context = context;
            Container = container;
            _timer = new Timer()
            {
                Interval = interval,
                Enabled = false,
            };

            _timer.Tick += Timer_Tick;
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

                    foreach (OLVColumn col in ListView.Columns)
                    {
                        col.MinimumWidth = 0;
                        col.AutoResize(ColumnHeaderAutoResizeStyle.HeaderSize);
                    }

                    foreach (OLVColumn col in ListView.Columns)
                    {
                        col.Width += 10;
                    }
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
        }

        private void Timer_Tick(object sender, EventArgs e)
        {
            if (ListView?.Visible == true)
            {
                var refresh = _newItem;

                if (AutoUpdateUntilContextLoaded)
                {
                    refresh = true;
                    if (Context?.FullyLoaded == true)
                    {
                        AutoUpdateUntilContextLoaded = false;
                    }
                }

                if (refresh)
                {
                    RefreshItems(true);
                }
            }
        }

        public void Start()
        {
            _timer.Start();
        }
    }
}