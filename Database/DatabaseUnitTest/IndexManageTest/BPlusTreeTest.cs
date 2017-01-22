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
using Database.RecordManage;

namespace DatabaseUnitTest.IndexManageTest
{
    [TestClass]
    public class BPlusTreeTest
    {
        [TestMethod]
        public void SearchAndAdd()
        {
            BPlusTree<int, NodeStringInt> bt = new BPlusTree<int, NodeStringInt>(4);

            var list = new List<NodeStringInt>();

            for (int i = 1; i <= 7; i++)
            {
                if (i != 9999)
                {
                    var node = new NodeStringInt(i);
                    bt.Insert(node);
                    bt.InsertRepair(null,null);
                }
            }
            bt.TraverseForword(bt.Root, bt.TraverseOutput);
            bt.Insert(new NodeStringInt(9999));
            bt.Search(546484);
            var v = bt.SearchNode;
            Assert.AreEqual(v, 546484);
        }

        [TestMethod]
        public void SearchAndAddRID()
        {
            BPlusTree<int, NodeRIDInt> bt = new BPlusTree<int, NodeRIDInt>(4);

            var list = new List<NodeStringInt>();

            for (int i = 1; i <= 7; i++)
            {
                if (i != 9999)
                {
                    RID rid1 = new RID(i,i);
                    var node = new NodeRIDInt(i, rid1);
                    bt.Insert(node);
                    bt.InsertRepair(null,null);
                }
            }
            bt.TraverseForword(bt.Root, bt.TraverseOutput);
            RID rid = new RID(9999, 9999);
            bt.Insert(new NodeRIDInt(9999, rid));
            bt.Search(546484);
            var v = bt.SearchNode;
            Assert.AreEqual(v, 546484);
        }

        [TestMethod]
        public void BPlusTree()
        {
            XmlConfigurator.Configure();

            BPlusTree<int, NodeStringInt> bt = new BPlusTree<int, NodeStringInt>(4);

            var list = new List<NodeStringInt>();

            for (int i = 0; i <= 10; i++)
            {
                Insert(i * 2 + 1, bt);
            }
            bt.TraverseForword(bt.Root, bt.TraverseOutput);
            Delete(5, bt);
            var list2 = bt.GetAllLeafNode();
            bt.TraverseForword(bt.Root, bt.TraverseOutput);
        }

        private void Insert(int i, BPlusTree<int, NodeStringInt> bt)
        {
            bt.Insert(new NodeStringInt(i));
            bt.InsertRepair(null,null);
        }

        private void Delete(int i, BPlusTree<int, NodeStringInt> bt)
        {
            bt.Delete(i);
            bt.RepairAfterDelete(null,null,null);
        }
    }
}
