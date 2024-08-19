using Npgsql;
using PG_ODBC_TABExchangeV2.LOGFileWriter;
using PG_ODBC_TABExchangeV2.ProgressBar;
using System;
using System.Collections.Generic;
using System.Data.OleDb;


namespace PG_ODBC_TABExchangeV2.Database
{
    class WebDB
    {
        private string _connectionStringToLocal;
        private string _connectionStringToWeb;
        private List<string> _tableList;
        private string logFilePath;
        string _tableName;
        public WebDB(string _connectionStringToLocal, string _connectionStringToWeb, List<string> _tableList, string logFilePath)
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
            int rowsCount;
            try
            {
                Console.WriteLine($"***Начало работы (экспорт) - {DateTime.Now}");
                for (int i = 0; i < _tableList.Count; i++)
                {
                    
                    logWiter.LogMessage(logFilePath, $"***{DateTime.Now}");
                    logWiter.LogMessage(logFilePath, $"Таблица {_tableName} экспорт в локальную базу данных.");

                    _tableName = _tableList[i];
                    if (TableExist())
                    {
                        logWiter.LogMessage(logFilePath, $"Таблица с идентичным названием {_tableName} существует.");

                        DropTable();

                        CreateTable(GetColumnsAndTypesForCreate());

                        rowsCount = Insert(i);

                        if (IsPrimaryKey())
                        {
                            SetPrimaryCounterToMaxValue();
                        }


                    }
                    else
                    {
                        CreateTable(GetColumnsAndTypesForCreate());

                        logWiter.LogMessage(logFilePath, $"Таблица {_tableName} успешно создана.");

                        rowsCount = Insert(i);

                        if (IsPrimaryKey())
                        {
                            SetPrimaryCounterToMaxValue();
                        }
                        
                    }
                    logWiter.LogMessage(logFilePath, $"Строк вставлено {rowsCount}");
                    logWiter.LogMessage(logFilePath, $"Данные успешно экспортированы в таблицу {_tableName}");
                    logWiter.LogMessage(logFilePath, $"Время завершения: {DateTime.Now}");
                }
                Console.WriteLine();
                Console.WriteLine($"***Работа завершена (экспорт) - {DateTime.Now}");
            }
            catch (Exception ex)
            {
                logWiter.ErrorLogMessage(logFilePath, $"Ошибка главного блока команд: {ex.Message}");
            }
        }

        public int Insert(int tableListIndex)
        {
            List<List<object>> rows = new List<List<object>>();
            int rowsCount = 0;
            int allRowsCounter = 0;

            try
            {
                using (OleDbConnection localConn = new OleDbConnection(_connectionStringToLocal))
                using (var webConn = new NpgsqlConnection(_connectionStringToWeb))
                {
                    webConn.Open();
                    localConn.Open();
                    using (OleDbCommand localCmd = new OleDbCommand($"SELECT COUNT(*) FROM {_tableName}", localConn))
                    {
                        allRowsCounter = (int)Convert.ToInt64(localCmd.ExecuteScalar());
                    }

                    using (OleDbCommand localCmd = new OleDbCommand($"SELECT * FROM {_tableName}", localConn))
                    using (OleDbDataReader reader = localCmd.ExecuteReader())
                    {
                        
                        while (reader.Read())
                        {
                            
                            List<object> row = new List<object>();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                if (!reader.IsDBNull(i))
                                {
                                    row.Add(reader.GetValue(i));
                                }
                                else
                                {
                                    row.Add(DBNull.Value);
                                }
                            }
                            rowsCount++;
                            if (rowsCount % 100 == 0 || rowsCount == allRowsCounter)
                            {
                                newBar.InsertProgressBar(_tableName, rowsCount, allRowsCounter, tableListIndex);
                            }
                            rows.Add(row);

                        }
                    }
                        using (NpgsqlBinaryImporter importer = webConn.BeginBinaryImport($"COPY {_tableName} FROM STDIN BINARY"))
                        {
                        foreach (var row in rows)
                        {
                            importer.StartRow();
                            foreach (var item in row)
                            {

                                if (item is Byte || item is byte[])
                                {

                                    importer.Write(item.ToString(), NpgsqlTypes.NpgsqlDbType.Text);
                                }
                                else
                                {
                                    importer.Write(item, GetNpgsqlDbType(item.GetType()));
                                }
                            }
                        }
                        importer.Complete();
                    }
                }

            }
            catch (Exception ex)
            {
                logWiter.ErrorLogMessage(logFilePath, $"Ошибка при вставке данных в таблицу {_tableName}: {ex.Message}");
            }

            return rowsCount; 
        }


        private static NpgsqlTypes.NpgsqlDbType GetNpgsqlDbType(Type type)
        {
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Empty:
                case TypeCode.Object:
                    if (type == typeof(TimeSpan))
                        return NpgsqlTypes.NpgsqlDbType.Interval;
                    else
                        return NpgsqlTypes.NpgsqlDbType.Text;
                case TypeCode.DBNull:
                    return NpgsqlTypes.NpgsqlDbType.Text;
                case TypeCode.Boolean:
                    return NpgsqlTypes.NpgsqlDbType.Boolean;
                case TypeCode.Char:
                    return NpgsqlTypes.NpgsqlDbType.Char;
                case TypeCode.SByte:
                case TypeCode.Byte:
                case TypeCode.Int16:
                case TypeCode.UInt16:
                case TypeCode.Int32:
                case TypeCode.UInt32:
                case TypeCode.Int64:
                case TypeCode.UInt64:
                    return NpgsqlTypes.NpgsqlDbType.Integer;
                case TypeCode.Single:
                case TypeCode.Double:
                    return NpgsqlTypes.NpgsqlDbType.Double;
                case TypeCode.Decimal:
                    return NpgsqlTypes.NpgsqlDbType.Numeric;
                case TypeCode.DateTime:
                    return NpgsqlTypes.NpgsqlDbType.Timestamp;
                case TypeCode.String:
                    return NpgsqlTypes.NpgsqlDbType.Text;
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
        }


        public bool TableExist()
        {
            bool result = false;
            try
            {
                using (var webConn = new NpgsqlConnection(_connectionStringToWeb))
                {
                    webConn.Open();
                    using (NpgsqlCommand webCmd = new NpgsqlCommand($"SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = '{_tableName}');", webConn))
                    {
                        result = Convert.ToBoolean(webCmd.ExecuteScalar());
                    }
                }
            }
            catch (Exception ex)
            {
                logWiter.ErrorLogMessage(logFilePath, $"Ошибка при проверке существования таблицы {_tableName}: {ex.Message}\n" +
                        $"Возможно некорректно указано название исходной таблицы");

            }
            return result;
        }
        public void DropTable()
        {
            using (var webConn = new NpgsqlConnection(_connectionStringToWeb))
            {
                webConn.Open();
                using (NpgsqlCommand webCmd = new NpgsqlCommand($"DROP TABLE \"{_tableName}\"", webConn))
                {
                    webCmd.ExecuteNonQuery();
                }
            }
        }
        public void CreateTable(string columns)
        {
            try
            {
                using (var localConn = new NpgsqlConnection(_connectionStringToWeb))
                {
                    localConn.Open();
                    using (NpgsqlCommand locCmd = new NpgsqlCommand($"CREATE TABLE \"{_tableName}\" ({columns});", localConn))
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
        public bool IsPrimaryKey()
        {
            string firstColumnName = "";
            bool result = false;
            try
            {
                using (var webConn = new NpgsqlConnection(_connectionStringToWeb))
                {
                    webConn.Open();
                    using (NpgsqlCommand webCmd = new NpgsqlCommand($@"SELECT * FROM {_tableName} LIMIT 1", webConn))
                    using (NpgsqlDataReader reader = webCmd.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            reader.Read();
                            firstColumnName = reader.GetName(0);
                        }
                    }
                    using (NpgsqlCommand webCmd = new NpgsqlCommand($@"SELECT indexdef FROM pg_indexes WHERE tablename = '{_tableName}' AND indexname LIKE '%pkey%'", webConn))
                    using (NpgsqlDataReader reader = webCmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            if (reader[0] != DBNull.Value && reader[0].ToString().Contains(firstColumnName))
                            {
                                result = true;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logWiter.ErrorLogMessage(logFilePath, $"Ошибка при проверке наличия у таблицы {_tableName} первичного ключа: {ex.Message}");
            }
            return result;
        }

        public void SetPrimaryCounterToMaxValue()
        {
            int maxValue = 0;
            string columnPrimary = "";
            using (var webConn = new NpgsqlConnection(_connectionStringToWeb))
            {
                webConn.Open();
                using (NpgsqlCommand webCmd = new NpgsqlCommand($@"SELECT * FROM {_tableName} LIMIT 1", webConn))
                using (NpgsqlDataReader reader = webCmd.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        reader.Read();
                        columnPrimary = reader.GetName(0);
                    }
                }
                using (NpgsqlCommand webCmd = new NpgsqlCommand($"SELECT MAX(\"{columnPrimary}\") FROM \"{_tableName}\"", webConn))
                {
                    var result = webCmd.ExecuteScalar();
                    if (result != null)
                    {
                        maxValue = Convert.ToInt32(result);
                    }
                }

                using (NpgsqlCommand webCmd = new NpgsqlCommand($"ALTER TABLE {_tableName} ALTER COLUMN {columnPrimary} RESTART WITH {maxValue + 1};", webConn))
                {
                    webCmd.ExecuteNonQuery();
                }
            }
        }
        public string GetColumnsAndTypesForCreate()
        {
            string columnsForCreateCommand = "";
            using (OleDbConnection localConn = new OleDbConnection(_connectionStringToLocal))
            {
                localConn.Open();

                using (OleDbCommand localCmd = new OleDbCommand($"SELECT TOP 1 * FROM {_tableName}", localConn))
                using (OleDbDataReader reader = localCmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            string columnName = reader.GetName(i);
                            string dataTypeName = reader.GetDataTypeName(i);
                            dataTypeName = Converter(dataTypeName);
                            if (i == 0 && dataTypeName == "SERIAL")
                            {
                                columnsForCreateCommand = "\"" + columnName + "\"" + " INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY";
                            }
                            else if (reader.IsDBNull(i))
                            {
                                if (dataTypeName == "SERIAL")
                                    dataTypeName = "INTEGER";
                                columnsForCreateCommand += (i > 0 ? ", " + "\"" + columnName + "\"" + " " + dataTypeName + " NULL" : "\"" + columnName + "\"" + " " + dataTypeName + " NULL");

                            }
                            else
                            {
                                if (dataTypeName == "SERIAL")
                                    dataTypeName = "INTEGER";
                                columnsForCreateCommand += (i > 0 ? ", " + "\"" + columnName + "\"" + " " + dataTypeName : "\"" + columnName + "\"" + " " + dataTypeName);
                            }

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
                case "DBTYPE_I4":
                    return "SERIAL";
                case "DBTYPE_I1":
                case "DBTYPE_I2":
                    return "INTEGER";
                case "DBTYPE_WVARCHAR":
                    return "varchar";
                case "DBTYPE_I8":
                    return "BIGINT";
                case "DBTYPE_R4":
                case "DBTYPE_R8":
                    return "DOUBLE PRECISION";
                case "DBTYPE_CY":
                    return "NUMERIC";
                case "DBTYPE_DATE":
                    return "TIMESTAMP";
                case "DBTYPE_BOOL":
                    return "BOOLEAN";
                case "DBTYPE_BSTR":
                case "DBTYPE_STR":
                case "DBTYPE_WSTR":
                case "DBTYPE_WLONGVARCHAR":
                    return "TEXT";
                case "DBTYPE_GUID":
                    return "UUID";
                case "DBTYPE_VARIANT":
                    return "JSONB";
                case "DBTYPE_IDISPATCH":
                case "DBTYPE_VARNUMERIC":
                    return "NUMERIC";
                case "DBTYPE_BYTES":
                case "DBTYPE_UI1":
                case "DBTYPE_LONGVARBINARY":
                case "DBTYPE_VARBINARY":
                    return "BYTEA";
                case "DBTYPE_FILETIME":
                    return "TIMESTAMP";
                case "DBTYPE_NUMERIC":
                    return "NUMERIC";
                case "DBTYPE_DECIMAL":
                    return "NUMERIC";
                case "DBTYPE_ERROR":
                    return "INTEGER";
                case "DBTYPE_PROPVARIANT":
                    return "JSONB";
                default:
                    throw new ArgumentException($"Unsupported data type: {dataTypeName}");
            }
        }
    }   
}
