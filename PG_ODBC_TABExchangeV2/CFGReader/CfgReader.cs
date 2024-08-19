using PG_ODBC_TABExchangeV2.LOGFileWriter;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text.RegularExpressions;


namespace PG_ODBC_TABExchangeV2.CFGReader
{
    public class CfgReader
    {
        private const string controlInfo = "****";

        private string dbNetPath = "";
        private string dbLocalPath = "";
        private string logFileName = "";
        private int waitSeconds = 0;
        List<string> tabLocalToNet = new List<string>();
        List<string> tabNetToLocal = new List<string>();
        public void Reader()
        {
            string executablePath = Assembly.GetExecutingAssembly().Location;

            if (string.IsNullOrEmpty(executablePath))
            {
                executablePath = AppContext.BaseDirectory;
            }

            string directoryPath = Path.GetDirectoryName(executablePath);

            CheckControlFile(directoryPath + "\\PG-ODBC-TABExchange.cfg");

            using (StreamReader sr = new StreamReader(directoryPath + "\\PG-ODBC-TABExchange.cfg"))
            {
                string line;
                Regex regex = new Regex(@"""([^""]*)""");
                while ((line = sr.ReadLine()) != null)
                {
                    Match match = regex.Match(line);
                    while (match.Success)
                    {
                        if (dbNetPath == "" && line.StartsWith("DB_NetPath"))
                        {
                            dbNetPath = match.Groups[1].Value;
                        }
                        else if (dbLocalPath == "" && line.StartsWith("DB_LocalPath"))
                        {
                            dbLocalPath = match.Groups[1].Value;
                        }
                        else if (line.StartsWith("TAB_LocalToNet"))
                        {
                            string[] values = match.Groups[1].Value.Split(',');
                            foreach (string value in values)
                            {
                                tabLocalToNet.Add(value.Trim('[', ']'));
                            }
                        }
                        else if (line.StartsWith("TAB_NetToLocal"))
                        {
                            string[] values = match.Groups[1].Value.Split(',');
                            foreach (string value in values)
                            {
                                tabNetToLocal.Add(value.Trim('[', ']'));
                            }
                        }
                        else if (logFileName == "" && line.StartsWith("LOG_FileName"))
                        {
                            logFileName = match.Groups[1].Value;
                        }
                        else if (line.StartsWith("Wait_Seconds"))
                        {
                            waitSeconds = Convert.ToInt16(match.Groups[1].Value);
                            
                        }
                        match = match.NextMatch();
                    }
                }
            }
        }
        private void CheckControlFile(string ControlfilePath)
        {
            LogWriter logWiter = new LogWriter();
            if (File.Exists(ControlfilePath))
            {

            }
            else
            {
                try
                {
                    using (StreamWriter controlWriter = new StreamWriter(ControlfilePath))
                    {
                        controlWriter.WriteLine(controlInfo);
                    }
                }
                catch (Exception ex)
                {
                    logWiter.ErrorLogMessage("Ошибка при создании файла управления:", ex.Message);
                }
            }
        }
        public string GetdbNetPath()
        {
            return dbNetPath;
        }
        public string GetdbLocalPath()
        {
            return dbLocalPath;
        }
        public List<string> GetLocalToNet()
        {
            return tabLocalToNet;
        }
        public List<string> GetNetToLocal()
        {
            return tabNetToLocal;
        }
        public string GetLogFilePath()
        {
            return logFileName;
        }
        public int GetSecondsForWait() 
        {
            return waitSeconds;
        }
    }
}
