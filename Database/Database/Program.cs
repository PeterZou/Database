﻿using System;
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

            BPlusTree<int, NodeInt> bt = new BPlusTree<int, NodeInt>(3);

            var list = new List<NodeInt>();

            for (int i = 1; i <= 3; i++)
            {
                bt.Insert(new NodeInt(i));
            }

            bt.Traverse(bt.Root, bt.TraverseOutput);

            bt.Delete(2);
            bt.Traverse(bt.Root, bt.TraverseOutput);

            Console.ReadKey();
        }
    }
}
