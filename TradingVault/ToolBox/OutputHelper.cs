using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ToolBox
{
    public class OutputHelper
    {
        private System.IO.FileInfo mLogFileName;
        private System.IO.FileInfo mExecutablePath;
        private object mLockPrint;

        public OutputHelper(string argvAppName)
        {
            mLockPrint = new object();
            Uri tExecutablePath = new Uri(System.Reflection.Assembly.GetEntryAssembly().GetName().CodeBase);
            mExecutablePath = new System.IO.FileInfo(tExecutablePath.AbsolutePath);
            mLogFileName = new System.IO.FileInfo(mExecutablePath.Directory.FullName + "\\Log\\" + argvAppName + "-" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt");
            mLogFileName.Directory.Create();
        }

        public void Print(string argvMessage)
        {
            lock (mLockPrint)
            {
                string tStrNow = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                Console.WriteLine(tStrNow + ": " + argvMessage);
            }
        }

        public void Log(string argvMessage)
        {
            this.Print(argvMessage);
            lock (mLockPrint)
            {
                string tStrNow = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                using (System.IO.StreamWriter tFileWriter = new System.IO.StreamWriter(mLogFileName.FullName, true))
                {
                    tFileWriter.WriteLine(tStrNow + ": " + argvMessage);
                }
            }
        }

        public void PrintDataTable(System.Data.DataTable argvTable)
        {
            string tPrintOut = string.Empty;

            //Print column names
            for (int i = 0; i < argvTable.Columns.Count; i++)
            {
                tPrintOut += argvTable.Columns[i].ColumnName + ",";
            }
            if(tPrintOut.Last() == ',')
            {
                tPrintOut = tPrintOut.Substring(0, tPrintOut.Length - 1);
            }
            tPrintOut += Environment.NewLine;

            //Print rows
            for (int i = 0; i < argvTable.Rows.Count; i++)
            {
                for (int j = 0; j < argvTable.Columns.Count; j++)
                {
                    tPrintOut += argvTable.Rows[i][j].ToString() + ",";
                }

                if (tPrintOut.Last() == ',')
                {
                    tPrintOut = tPrintOut.Substring(0, tPrintOut.Length - 1);
                }
                tPrintOut += Environment.NewLine;
            }

            this.Print(tPrintOut);
        }
    }
}
