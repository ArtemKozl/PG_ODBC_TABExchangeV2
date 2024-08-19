using System;

namespace PG_ODBC_TABExchangeV2.ProgressBar
{
    class ProgressBarClass
    {
        public void InsertProgressBar(string _tableName, int rowsCount, double allRowsCounter, int tableListIndex) 
        {
            Console.SetCursorPosition(0, tableListIndex + 1);
            double percentage = Math.Ceiling((rowsCount / allRowsCounter) * 100);

            int sharpCount = (int)(percentage / 10);
            string progressBar = new string('#', sharpCount) + new string(' ', 10 - sharpCount);
            Console.Write($"{_tableName}:[{progressBar}] {percentage}%");
        }
    }
}
