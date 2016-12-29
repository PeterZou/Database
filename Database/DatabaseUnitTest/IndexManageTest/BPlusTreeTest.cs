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
                if (i != 9999)
                {
                    var node = new NodeInt(i);
                    bt.Insert(node);
                    bt.InsertRepair(true,null,null);
                }
            }
            bt.TraverseForword(bt.Root, bt.TraverseOutput);
            //bt.Insert(new NodeInt(9999));
            //bt.Search(546484);
            //var v = bt.SearchNode;
            //Assert.AreEqual(v, 546484);
        }

        [TestMethod]
        public void BPlusTree()
        {
            XmlConfigurator.Configure();

            BPlusTree<int, NodeInt> bt = new BPlusTree<int, NodeInt>(3);

            var list = new List<NodeInt>();

            for (int i = 1; i <= 6; i++)
            {
                bt.Insert(new NodeInt(i));
                bt.InsertRepair(true,null,null);

            }
            bt.TraverseForword(bt.Root, bt.TraverseOutput);
            bt.Delete(1);
            bt.RepairAfterDelete();
            bt.TraverseForword(bt.Root, bt.TraverseOutput);
        }
    }
}
