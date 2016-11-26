using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Database.IndexManage.BPlusTree;
using Database.IndexManage.IndexValue;
using System.Diagnostics;
using log4net.Config;

namespace DatabaseUnitTest.IndexManageTest
{
    [TestClass]
    public class BPlusTreeTest
    {
        [TestMethod]
        public void SearchAndAdd()
        {
            BPlusTree<int, NodeInt> bt = new BPlusTree<int, NodeInt>(4);

            var list = new List<NodeInt>();

            for (int i = 1; i <= 7; i++)
            {
                //if (i != 9999)
                    //bt.Insert(new NodeInt(i));
            }
            //bt.Traverse(bt.Root, bt.TraverseOutput);
            //bt.Insert(new NodeInt(9999));
            bt.Search(546484);
            var v = bt.SearchNode;
            Assert.AreEqual(v, 546484);
        }

        [TestMethod]
        public void BPlusTree()
        {
            XmlConfigurator.Configure();

            Stopwatch sw = new Stopwatch();
            sw.Start();

            BPlusTree<int, NodeInt> bt = new BPlusTree<int, NodeInt>(3);

            var list = new List<NodeInt>();

            for (int i = 1; i <= 6; i++)
            {
                bt.Insert(new NodeInt(i));
                bt.InsertRepair();

            }
            bt.Traverse(bt.Root, bt.TraverseOutput);
            sw.Stop();
            Console.WriteLine(sw.Elapsed);
            bt.Delete(1);
            bt.RepairAfterDelete();
            bt.Traverse(bt.Root, bt.TraverseOutput);
            Console.ReadKey();
        }
    }
}
