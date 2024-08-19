using PG_ODBC_TABExchangeV2.CFGReader;
using PG_ODBC_TABExchangeV2.Database;
using System;
using System.Collections.Generic;
using System.Threading;


namespace PG_ODBC_TABExchangeV2
{
    class Program
    {
        static void Main(string[] args)
        {

            CfgReader readerCFG = new CfgReader();
            readerCFG.Reader();
            string connectionStringToLocal = readerCFG.GetdbLocalPath();
            string connectionStringToWeb = readerCFG.GetdbNetPath();
            string logFilePath = readerCFG.GetLogFilePath();
            List<string> listLocalToNet = new List<string>(readerCFG.GetLocalToNet());
            List<string> listNetToLocal = new List<string>(readerCFG.GetNetToLocal());

            WebDB webDB = new WebDB(connectionStringToLocal,connectionStringToWeb,listLocalToNet, logFilePath);
            LocalDB locDB = new LocalDB(connectionStringToLocal, connectionStringToWeb, listNetToLocal, logFilePath);
            

            if (listLocalToNet[0] != string.Empty)
                webDB.Main();
            if (listNetToLocal[0] != string.Empty)
            {
                Console.Clear();
                locDB.Main();
            }

            Thread closeThread = new Thread(() =>
            {
                Thread.Sleep(readerCFG.GetSecondsForWait() * 1000);
                Environment.Exit(0);
            });
            closeThread.Start();

            ConsoleKeyInfo keyInfo;
            do
            {
                keyInfo = Console.ReadKey(true);
            } while (keyInfo.Key == ConsoleKey.NoName);

            if (closeThread.IsAlive)
            {
                closeThread.Abort();
            }

        }

    }
}
