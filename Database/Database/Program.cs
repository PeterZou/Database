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
using Database.IndexManage;
using System.Diagnostics;

namespace Database
{
    class Program
    {
        static void Main(string[] args)
        {
            XmlConfigurator.Configure();

            BPlusTree<int, NodeInt> bt = new BPlusTree<int, NodeInt>(4);

            Stopwatch s = new Stopwatch();
            s.Start();
            var list = new List<NodeInt>();

            for (int i = 1; i <= 1000000; i++)
            {
                if(i != 9999)
                    bt.Insert(new NodeInt(i));
            }
            s.Stop();
            Console.WriteLine(s.Elapsed);
            //bt.Traverse(bt.Root, bt.TraverseOutput);
            s.Restart();
            bt.Insert(new NodeInt(9999));
            s.Stop();
            Console.WriteLine(s.Elapsed);
            s.Restart();
            bt.Search(546484);
            var v = bt.SearchNode;
            s.Stop();
            Console.WriteLine(s.Elapsed);
            Console.ReadKey();
        }
    }
}
