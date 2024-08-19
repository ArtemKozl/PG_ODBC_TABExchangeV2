using System;
using System.IO;


namespace PG_ODBC_TABExchangeV2.LOGFileWriter
{
    class LogWriter
    {
        public void LogMessage(string logFilePath, string message)
        {
            using (StreamWriter sw = new StreamWriter(logFilePath, true))
            {
                sw.WriteLine(message);
            }
        }
        public void ErrorLogMessage(string logFilePath, string message)
        {
            using (StreamWriter sw = new StreamWriter(logFilePath, true))
            {
                sw.WriteLine(message);
            }
            Environment.Exit(1);
        }
    }
}
