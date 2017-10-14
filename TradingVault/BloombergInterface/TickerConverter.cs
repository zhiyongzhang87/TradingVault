using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BloombergInterface
{
    public class TickerConverter
    {
        private Dictionary<string, string> mTickerLookupTable;
        private Dictionary<string, int> mStrikeMultiplier;
        private Dictionary<int, string> mMonthCodes;

        public TickerConverter()
        {
            mTickerLookupTable = new Dictionary<string, string>();
            mStrikeMultiplier = new Dictionary<string, int>();
            Uri tExecutableUri = new Uri(System.Reflection.Assembly.GetEntryAssembly().GetName().CodeBase);
            System.IO.FileInfo tExecutablePath = new System.IO.FileInfo(tExecutableUri.AbsolutePath);
            System.IO.FileInfo tLookupTableFilePath = new System.IO.FileInfo(tExecutablePath.Directory.FullName + "\\TickerLookupTable.txt");
            System.IO.StreamReader tFileReader = new System.IO.StreamReader(tLookupTableFilePath.FullName);
            string tLine = string.Empty;
            while ((tLine = tFileReader.ReadLine()) != null)
            {
                string[] tSplitParts = tLine.Split(':');
                if(tSplitParts.Length == 3)
                {
                    mStrikeMultiplier.Add(tSplitParts[0], int.Parse(tSplitParts[1]));
                    mTickerLookupTable.Add(tSplitParts[0], tSplitParts[2]);
                }
            }

            mMonthCodes = new Dictionary<int, string>();
            mMonthCodes.Add(1, "F");
            mMonthCodes.Add(2, "G");
            mMonthCodes.Add(3, "H");
            mMonthCodes.Add(4, "J");
            mMonthCodes.Add(5, "K");
            mMonthCodes.Add(6, "M");
            mMonthCodes.Add(7, "N");
            mMonthCodes.Add(8, "Q");
            mMonthCodes.Add(9, "U");
            mMonthCodes.Add(10, "V");
            mMonthCodes.Add(11, "X");
            mMonthCodes.Add(12, "Z");
        }

        public string BloombergTicker(string argvTicker)
        {
            string tBloombergTicker = string.Empty;
            string[] tSplitParts = argvTicker.Split('-');
            string tSymbol = tSplitParts[0];
            int tYear = 0;
            int tYearLastDigit = 0;
            int tMonth = 0;
            string tMonthCode = string.Empty;
            string tOptionType = string.Empty;
            double tStrike = 0;

            if (mTickerLookupTable.ContainsKey(tSymbol))
            {
                int tStrikeMultiplier = mStrikeMultiplier[tSymbol];

                if (tSplitParts.Length > 1)
                {
                    tYear = int.Parse(tSplitParts[1].Substring(0, 4));
                    tYearLastDigit = tYear - tYear / 10 * 10;
                    tMonth = int.Parse(tSplitParts[1].Substring(4, 2));
                    tMonthCode = mMonthCodes[tMonth];
                    if (tSplitParts.Length > 2)
                    {
                        tOptionType = tSplitParts[2];
                        tStrike = double.Parse(tSplitParts[3]) / tStrikeMultiplier;
                    }
                }

                tBloombergTicker = mTickerLookupTable[tSymbol];
                tBloombergTicker = tBloombergTicker.Replace("{MonthCode}", tMonthCode);
                tBloombergTicker = tBloombergTicker.Replace("{YearLastDigit}", tYearLastDigit.ToString());
                tBloombergTicker = tBloombergTicker.Replace("{OptionType}", tOptionType);
                tBloombergTicker = tBloombergTicker.Replace("{Strike}", tStrike.ToString());
            }

            return tBloombergTicker;
        }
    }
}
