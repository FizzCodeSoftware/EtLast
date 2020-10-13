namespace FizzCode.EtLast.EPPlus
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using FizzCode.EtLast;
    using OfficeOpenXml;

    public class EpPlusSimpleRowWriterMutator : AbstractMutator, IRowWriter
    {
        public string FileName { get; set; }
        public ExcelPackage ExistingPackage { get; set; }
        public string SheetName { get; set; }
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public Action<ExcelPackage, SimpleExcelWriterState> Finalize { get; set; }

        private SimpleExcelWriterState _state;
        private ExcelPackage _package;
        private int? _storeUid;

        public EpPlusSimpleRowWriterMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void StartMutator()
        {
            _state = new SimpleExcelWriterState();
        }

        protected override void CloseMutator()
        {
            if (_state.LastWorksheet != null)
            {
                Finalize?.Invoke(_package, _state);
            }

            if (ExistingPackage == null && _package != null)
            {
                var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.fileWrite, PathHelpers.GetFriendlyPathName(FileName), null, null, null, null,
                    "saving file to {FileName}",
                    PathHelpers.GetFriendlyPathName(FileName));

                try
                {
                    _package.Save();
                    Context.RegisterIoCommandSuccess(this, IoCommandKind.fileWrite, iocUid, null);
                }
                catch (Exception ex)
                {
                    Context.RegisterIoCommandFailed(this, IoCommandKind.fileWrite, iocUid, null, ex);
                    throw;
                }

                _package.Dispose();
                _package = null;
            }

            _state = null;
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (_storeUid == null)
            {
                _storeUid = Context.GetStoreUid(PathHelpers.GetFriendlyPathName(FileName), SheetName);
            }

            Context.RegisterRowStored(this, row, _storeUid.Value);

            if (_package == null) // lazy load here instead of prepare
            {
#pragma warning disable CA2000 // Dispose objects before losing scope
                _package = ExistingPackage ?? new ExcelPackage(new FileInfo(FileName));
#pragma warning restore CA2000 // Dispose objects before losing scope

                _state.LastWorksheet = _package.Workbook.Worksheets.Add(SheetName);
                _state.LastRow = 1;
                _state.LastCol = 1;
                foreach (var col in ColumnConfiguration)
                {
                    _state.LastWorksheet.Cells[_state.LastRow, _state.LastCol].Value = col.ToColumn;
                    _state.LastCol++;
                }

                _state.LastRow++;
            }

            try
            {
                _state.LastCol = 1;
                foreach (var col in ColumnConfiguration)
                {
                    _state.LastWorksheet.Cells[_state.LastRow, _state.LastCol].Value = row[col.FromColumn];
                    _state.LastCol++;
                }

                _state.LastRow++;
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, row, "error raised during writing an excel file", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel file, file name: {0}, message: {1}, row: {2}", FileName, ex.Message, row.ToDebugString()));
                exception.Data.Add("FileName", FileName);
                exception.Data.Add("SheetName", SheetName);
                throw exception;
            }

            yield return row;
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (string.IsNullOrEmpty(FileName))
                throw new ProcessParameterNullException(this, nameof(FileName));

            if (string.IsNullOrEmpty(SheetName))
                throw new ProcessParameterNullException(this, nameof(SheetName));

            if (ColumnConfiguration == null)
                throw new ProcessParameterNullException(this, nameof(ColumnConfiguration));
        }
    }
}