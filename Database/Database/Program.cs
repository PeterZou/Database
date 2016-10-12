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

namespace Database
{
    class Program
    {
        static void Main(string[] args)
        {
            XmlConfigurator.Configure();

            BPlusTree<int, int> bt = new BPlusTree<int, int>();

            bt.Insert(1);

            Console.ReadKey();
        }
    }
}
