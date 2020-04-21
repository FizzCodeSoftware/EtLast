﻿namespace FizzCode.EtLast.Diagnostics.Windows
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
        public DiagContext Context { get; }
        public Control Container { get; }
        public int Interval { get => _timer.Interval; set => _timer.Interval = value; }
        public ObjectListView ListView { get; }
        public Func<TItem, bool> ItemFilter { get; set; }
        public TextBox SearchBox { get; private set; }
        public bool AutoUpdateUntilContextLoaded { get; set; }

        private readonly Timer _timer;
        private readonly List<TItem> _allItems = new List<TItem>();
        private bool _newItem;

        public ControlUpdater(DiagContext context, Control container, int interval = 1000)
        {
            Context = context;
            Container = container;
            _timer = new Timer()
            {
                Interval = interval,
                Enabled = false,
            };

            ListView = CreateListView(container);

            _timer.Tick += Timer_Tick;
        }

#pragma warning disable RCS1158 // Static member in generic type should use a type parameter.
        public static ObjectListView CreateListView(Control container)
#pragma warning restore RCS1158 // Static member in generic type should use a type parameter.
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
        }

#pragma warning disable RCS1158 // Static member in generic type should use a type parameter.
        public static void ResizeListView(ObjectListView listView)
#pragma warning restore RCS1158 // Static member in generic type should use a type parameter.
        {
            foreach (OLVColumn col in listView.Columns)
            {
                col.MinimumWidth = 0;
                col.AutoResize(ColumnHeaderAutoResizeStyle.ColumnContent);
            }

            foreach (OLVColumn col in listView.Columns)
            {
                col.Width += 5;
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
        }

        public void Start()
        {
            _timer.Start();
        }
    }
}