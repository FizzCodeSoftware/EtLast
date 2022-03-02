namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.IO;
    using OfficeOpenXml;

    public sealed class EpPlusPreLoadedExcelReader : AbstractEpPlusExcelReader
    {
        /// <summary>
        /// Usage: reader.PreLoadedFile = new ExcelPackage(new FileInfo(fileName));
        /// </summary>
        public ExcelPackage PreLoadedFile { get; init; }

        public EpPlusPreLoadedExcelReader(IEtlContext context)
            : base(context)
        {
        }

        public override string GetTopic()
        {
            if (PreLoadedFile.File?.Name != null)
                return Path.GetFileName(PreLoadedFile.File.Name);

            return null;
        }

        protected override void ValidateImpl()
        {
            if (PreLoadedFile == null)
                throw new ProcessParameterNullException(this, nameof(PreLoadedFile));

            if (string.IsNullOrEmpty(SheetName) && SheetIndex == -1)
                throw new ProcessParameterNullException(this, nameof(SheetName));

            if (Columns == null)
                throw new ProcessParameterNullException(this, nameof(Columns));
        }

        protected override IEnumerable<IRow> Produce()
        {
            return ProduceFrom(null, PreLoadedFile);
        }
    }
}