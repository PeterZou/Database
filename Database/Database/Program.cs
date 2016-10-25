using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.BufferManage;
using log4net;
using log4net.Config;
using System.Reflection;
using Database.FileManage;
using Database.Const;
using System.Diagnostics;
using Database.IndexManage.BPlusTree;
using Database.IndexManage.IndexValue;

namespace Database
{
    class Program
    {
        static void Main(string[] args)
        {
            XmlConfigurator.Configure();

            Stopwatch sw = new Stopwatch();
            sw.Start();

            BPlusTree<int, NodeInt> bt = new BPlusTree<int, NodeInt>(10);

            var list = new List<NodeInt>();

            for (int i = 1; i <= 1000000; i++)
            {
                if (i != 9999)
                    bt.Insert(new NodeInt(i));
            }
            //bt.Traverse(bt.Root, bt.TraverseOutput);
            sw.Stop();
            Console.WriteLine(sw.Elapsed);
            //bt.Traverse(bt.Root, bt.TraverseOutput);
            bt.Insert(new NodeInt(9999));
            bt.Search(546484);

            Console.ReadKey();
        }
    }
}
