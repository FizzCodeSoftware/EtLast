﻿namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Drawing;
    using System.Windows.Forms;
    using BrightIdeasSoftware;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class ContextRowStoreControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public Control Container { get; }
        public AbstractDiagContext Context { get; }
        public TrackedStore Store { get; }
        public ObjectListView ListView { get; }
        public TextBox SearchBox { get; }
        private readonly Dictionary<string, int> _columnIndexes = new Dictionary<string, int>();
        private readonly int _fixColumnCount;

        public ContextRowStoreControl(Control container, AbstractDiagContext context, TrackedStore store)
        {
            Container = container;
            Context = context;
            Store = store;

            container.SizeChanged += Container_SizeChanged;

            SearchBox = new TextBox()
            {
                Parent = container,
                Bounds = new Rectangle(10, 10, 150, 20),
            };

            SearchBox.TextChanged += SearchBox_TextChanged;

            ListView = new FastObjectListView()
            {
                Parent = container,
                BorderStyle = BorderStyle.FixedSingle,
                ShowItemToolTips = true,
                ShowGroups = false,
                UseFiltering = true,
                ShowCommandMenuOnRightClick = true,
                ShowFilterMenuOnRightClick = true,
                FullRowSelect = true,
                UseAlternatingBackColors = true,
                HeaderUsesThemes = true,
                GridLines = true,
                AlternateRowBackColor = Color.FromArgb(240, 240, 240),
                FilterMenuBuildStrategy = new CustomFilterMenuBuilder()
                {
                    MaxObjectsToConsider = int.MaxValue,
                },
            };

            ListView.AllColumns.Add(new OLVColumn()
            {
                Text = "UID",
                AspectGetter = x => (x as Model)?.UID,
                AspectToStringConverter = x => x == null
                    ? null
                    : ((int)x).FormatToStringNoZero(),
            });
            ListView.AllColumns.Add(new OLVColumn()
            {
                Text = "Process",
                AspectGetter = x => (x as Model).ProcessName,
            });
            ListView.Columns.AddRange(ListView.AllColumns.ToArray());
            _fixColumnCount = ListView.Columns.Count;
        }

        private void SearchBox_TextChanged(object sender, EventArgs e)
        {
            ListView.ModelFilter = TextMatchFilter.Contains(ListView, SearchBox.Text);
        }

        private void Container_SizeChanged(object sender, EventArgs e)
        {
            ListView.Bounds = new Rectangle(0, 40, Container.ClientSize.Width, Container.ClientSize.Height - 40);
        }

        public void Refresh()
        {
            ListView.BeginUpdate();
            try
            {
                ListView.Items.Clear();
                var modelList = new List<Model>();

                var newColumns = new List<OLVColumn>();

                Context.EnumerateThroughStoredRows(Store.UID, evt =>
                {
                    if (!Context.WholePlaybook.ProcessList.TryGetValue(evt.ProcessInvocationUID, out var process))
                        return;

                    for (var i = 0; i < evt.Values.Length; i++)
                    {
                        var columnName = evt.Values[i].Key;
                        if (!_columnIndexes.TryGetValue(columnName, out var columnIndex))
                        {
                            columnIndex = ListView.AllColumns.Count - _fixColumnCount;

                            var newColumn = new OLVColumn()
                            {
                                Text = columnName,
                                AspectGetter = x => (x as Model).Values[columnIndex],
                                AspectToStringConverter = FormattingHelpers.ToDisplayValue,
                            };

                            ListView.AllColumns.Add(newColumn);
                            newColumns.Add(newColumn);

                            newColumn = new OLVColumn()
                            {
                                Text = "",
                                AspectGetter = x => (x as Model).Values[columnIndex]?.GetType(),
                                AspectToStringConverter = value => ((Type)value)?.GetFriendlyTypeName(),
                            };

                            ListView.AllColumns.Add(newColumn);
                            newColumns.Add(newColumn);

                            _columnIndexes.Add(columnName, columnIndex);
                        }
                    }

                    var model = new Model()
                    {
                        UID = evt.RowUid,
                        ProcessName = process.DisplayName,
                        Values = new object[ListView.AllColumns.Count - _fixColumnCount],
                        Types = new string[ListView.AllColumns.Count - _fixColumnCount],
                    };

                    for (var i = 0; i < evt.Values.Length; i++)
                    {
                        var kvp = evt.Values[i];
                        var columnIndex = _columnIndexes[kvp.Key];

                        model.Values[columnIndex] = kvp.Value;
                        model.Types[columnIndex] = kvp.Value?.GetType().GetFriendlyTypeName();
                    }

                    modelList.Add(model);
                });

                ListView.Columns.AddRange(newColumns.ToArray());
                ListView.SetObjects(modelList);

                foreach (OLVColumn col in ListView.Columns)
                {
                    col.MinimumWidth = 0;
                    col.AutoResize(ColumnHeaderAutoResizeStyle.HeaderSize);
                }

                foreach (OLVColumn col in ListView.Columns)
                {
                    col.Width += 20;
                }
            }
            finally
            {
                ListView.EndUpdate();
            }
        }

        private class Model
        {
            public int UID { get; set; }
            public string ProcessName { get; set; }
            public object[] Values { get; set; }
            public string[] Types { get; set; }
        }
    }
}