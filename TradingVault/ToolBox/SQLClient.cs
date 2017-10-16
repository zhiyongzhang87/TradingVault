using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace ToolBox
{
    public class SqlResult
    {
        public DataTable mData;
        public Exception mError;
        public int mRowsAffected;

        public SqlResult()
        {
            mData = new DataTable();
            mError = null;
            mRowsAffected = 0;
        }
    }

    public class SqlUser
    {
        public enum xSqlExecutionType { Read = 1, Write = 2 };

        private SqlConnection mConnection;
        private object mLock;

        public SqlUser(string argvConnStr)
        {
            mConnection = new SqlConnection(argvConnStr);
            mLock = new object();
        }

        public SqlResult ValidateConnection()
        {
            SqlResult tReturnValue = new SqlResult();

            lock (mLock)
            {
                try
                {
                    mConnection.Open();
                    mConnection.Close();
                }
                catch (Exception e)
                {
                    tReturnValue.mError = e;
                    mConnection.Close();
                }
            }

            return tReturnValue;
        }

        public SqlResult ExecuteQuery(string argvQuery, xSqlExecutionType argvType)
        {
            SqlCommand tCmd = new SqlCommand();
            SqlDataReader tReader;
            SqlResult tReturnValue = new SqlResult();

            lock (mLock)
            {
                tCmd.CommandText = argvQuery;
                tCmd.CommandType = System.Data.CommandType.Text;
                tCmd.Connection = mConnection;

                try
                {
                    mConnection.Open();
                    if (argvType == xSqlExecutionType.Read)
                    {
                        tReader = tCmd.ExecuteReader();
                        tReturnValue.mData.Load(tReader);
                        tReader.Close();
                    }
                    else
                    {
                        tReturnValue.mRowsAffected = tCmd.ExecuteNonQuery();
                    }
                    mConnection.Close();
                }
                catch (Exception e)
                {
                    tReturnValue.mError = e;
                    mConnection.Close();
                }
            }

            return tReturnValue;
        }
    }
}
