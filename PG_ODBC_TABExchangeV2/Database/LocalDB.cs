using Npgsql;
using PG_ODBC_TABExchangeV2.ProgressBar;
using PG_ODBC_TABExchangeV2.LOGFileWriter;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Text;


namespace PG_ODBC_TABExchangeV2.Database
{
    class LocalDB
    {
        private string _connectionStringToLocal;
        private string _connectionStringToWeb;
        private List<string> _tableList;
        private string logFilePath;
        string _tableName;
        public LocalDB(string _connectionStringToLocal, string _connectionStringToWeb, List<string> _tableList, string logFilePath)
        {
            this._connectionStringToLocal = _connectionStringToLocal;
            this._connectionStringToWeb = _connectionStringToWeb;
            this._tableList = _tableList;
            this.logFilePath = logFilePath;
        }

        LogWriter logWiter = new LogWriter();
        ProgressBarClass newBar = new ProgressBarClass();
        public void Main()
        {
            
            try
            {
                Console.WriteLine($"***Начало работы (импорт) - {DateTime.Now}");
                for (int i = 0; i < _tableList.Count; i++)
                {
                    logWiter.LogMessage(logFilePath, $"***{DateTime.Now}");
                    logWiter.LogMessage(logFilePath, $"Таблица {_tableName} импорт в локальную базу данных.");

                    _tableName = _tableList[i];
                    if (TableExist())
                    {
                        logWiter.LogMessage(logFilePath, $"Таблица с идентичным названием {_tableName} существует.");

                        DropTable();

                        CreateTable(GetColumnsAndTypesForCreate());

                        logWiter.LogMessage(logFilePath, $"Таблица {_tableName} успешно создана.");

                        
                        int rowsCount = Insert(i);

                        logWiter.LogMessage(logFilePath, $"Строк вставлено {rowsCount}");
                        logWiter.LogMessage(logFilePath, $"Данные успешно экспортированы в таблицу {_tableName}");

                    }
                    else
                    {
                        CreateTable(GetColumnsAndTypesForCreate());

                        logWiter.LogMessage(logFilePath, $"Таблица {_tableName} успешно создана.");

                        int rowsCount = Insert(i);

                        logWiter.LogMessage(logFilePath, $"Строк вставлено {rowsCount}");
                        logWiter.LogMessage(logFilePath, $"Данные успешно экспортированы в таблицу {_tableName}");
                    }
                    logWiter.LogMessage(logFilePath, $"Импорт таблицы {_tableName} прошел успешно. Время завершения: {DateTime.Now}");
                }
                Console.WriteLine();
                Console.WriteLine($"***Работа завершена (импорт) - {DateTime.Now}");
            }
            catch (Exception ex) 
            {
                logWiter.ErrorLogMessage(logFilePath, $"Ошибка главного блока команд: {ex.Message}");
            }
        }
        public bool TableExist()
        {
            bool result;

            using (OleDbConnection localConn = new OleDbConnection(_connectionStringToLocal))
            {
                localConn.Open();
                try
                {
                    using (OleDbCommand localCmd = new OleDbCommand($"SELECT TOP 1 * FROM {_tableName};", localConn))
                    {
                        localCmd.ExecuteNonQuery();
                        result = true;
                    }
                }
                catch
                {
                    result = false;
                }
            }
            return result;
        }
        public void DropTable()
        {
            using (OleDbConnection localConn = new OleDbConnection(_connectionStringToLocal))
            {
                localConn.Open();
                using (OleDbCommand localCmd = new OleDbCommand($"DROP TABLE {_tableName}", localConn))
                {
                    localCmd.ExecuteNonQuery();
                }
            }
        }
        public int Insert(int tableListIndex)
        {
            List<object> values = new List<object>();
            string columnPrimary = "";
            string valueParametrName = "";
            int rowsCount = 0;
            double allRowsCounter = 0;
            try
            {
                using (OleDbConnection localConn = new OleDbConnection(_connectionStringToLocal))
                using (var webConn = new NpgsqlConnection(_connectionStringToWeb))
                {
                    localConn.Open();
                    webConn.Open();
                    
                    using (NpgsqlCommand webCmd = new NpgsqlCommand($"SELECT COUNT(*) FROM {_tableName}", webConn))
                    {
                        allRowsCounter = (int)Convert.ToInt64(webCmd.ExecuteScalar());

                    }

                    using (NpgsqlCommand webCmd = new NpgsqlCommand($"SELECT * FROM {_tableName}", webConn))
                    using (NpgsqlDataReader reader = webCmd.ExecuteReader())
                    {
                        columnPrimary = reader.GetName(0);
                        while (reader.Read())
                        {
                            valueParametrName = "";
                            values.Clear();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                values.Add(reader.GetValue(i));
                                valueParametrName += (i > 0 ? ", " : "") + "@value" + i;
                            }

                            using (OleDbCommand localCmd = new OleDbCommand($"INSERT INTO {_tableName} VALUES ({valueParametrName})", localConn))
                            {
                                for (int i = 0; i != values.Count; i++)
                                {
                                    Type valueType = values[i].GetType();
                                    if (valueType == typeof(float) || valueType == typeof(double) || valueType == typeof(decimal))
                                    {
                                        values[i] = values[i].ToString();
 
                                    }
                                    else if (valueType == typeof(byte[])) 
                                    {
                                        values[i] = Encoding.UTF8.GetString((byte[])values[i]);
                                    }

                                    localCmd.Parameters.AddWithValue("value"+i, values[i]);
                                    
                                }

                                localCmd.ExecuteNonQuery();

                                rowsCount++;
                                if (rowsCount%100==0 || rowsCount == allRowsCounter)
                                {
                                    newBar.InsertProgressBar(_tableName,rowsCount, allRowsCounter,tableListIndex);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logWiter.ErrorLogMessage(logFilePath, $"Ошибка при вставке данных в таблицу {_tableName}: {ex.Message}");
            }
            return rowsCount;
        }
        public void CreateTable(string columns)
        {
            try
            {
                using (OleDbConnection localConn = new OleDbConnection(_connectionStringToLocal))
                {
                    localConn.Open();
                    using (OleDbCommand locCmd = new OleDbCommand($"CREATE TABLE {_tableName} ({columns});", localConn))
                    {
                        locCmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                logWiter.ErrorLogMessage(logFilePath, $"Ошибка создания таблицы: {ex.Message}");
            }
        }
        public string GetColumnsAndTypesForCreate()
        {
            string columnsForCreateCommand = "";
            using (var destConn = new NpgsqlConnection(_connectionStringToWeb))
            {
                destConn.Open();

                using (NpgsqlCommand destCmd = new NpgsqlCommand($"SELECT * FROM {_tableName} LIMIT 1;", destConn))
                using (NpgsqlDataReader reader = destCmd.ExecuteReader())
                {

                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        string columnName = reader.GetName(i);
                        string dataTypeName = reader.GetDataTypeName(i);
                        dataTypeName = Converter(dataTypeName);
                        if (i == 0 && dataTypeName == "LONG")
                        {
                            columnsForCreateCommand = "[" + columnName + "]" + " COUNTER PRIMARY KEY";
                        }
                        else
                        {
                            columnsForCreateCommand += (i > 0 ? ", [" + columnName + "] " + dataTypeName + " NULL" : "[" + columnName + "] " + dataTypeName + " NULL");
                        }
                    }
                    return columnsForCreateCommand;
                }
            }
        }
        public string Converter(string dataTypeName)
        {
            switch (dataTypeName)
            {
                case "integer":
                case "int4":
                    dataTypeName = "LONG";
                    break;
                case "varchar":
                case "character varying":
                    dataTypeName = "TEXT";
                    break;
                case "timestamp":
                case "timestamp without time zone":
                    dataTypeName = "DATETIME";
                    break;
                case "numeric":
                    dataTypeName = "CURRENCY";
                    break;
                case "bool":
                case "boolean":
                    dataTypeName = "YESNO";
                    break;
                case "text":
                    dataTypeName = "MEMO";
                    break;
                case "smallint":
                    dataTypeName = "SHORT";
                    break;
                case "uuid":
                    dataTypeName = "GUID";
                    break;
                case "real":
                    dataTypeName = "SINGLE";
                    break;
                case "double precision":
                    dataTypeName = "DOUBLE";
                    break;
                case "bytea":
                    dataTypeName = "BYTE";
                    break;
                case "char":
                    dataTypeName = "CHAR";
                    break;
                case "serial":
                    dataTypeName = "AUTOINCREMENT";
                    break;
                default:
                    throw new Exception($"Unsupported data type: {dataTypeName}");
            }
            return dataTypeName;
        }
    }
}
