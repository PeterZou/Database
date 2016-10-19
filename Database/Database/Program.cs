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

namespace Database
{
    class Program
    {
        static void Main(string[] args)
        {
            XmlConfigurator.Configure();

            var bBplusTree = BPlusTreeProvider<int, NodeInt>.GetBBplusTree(3);
  
            Console.ReadKey();
        }
    }
}
