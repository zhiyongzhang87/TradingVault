using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BloombergInterface;

namespace TradingVault
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.SetBufferSize(Console.BufferWidth, 23766);
            Tester tTester = new Tester();
            Console.ReadLine();
            tTester.ShutDown();
        }
    }
}
