using System.Linq;
using System.Text;
using System;
using System.Collections.Generic;
using Database.IndexManage.BPlusTree;
using Database.IndexManage.IndexValue;

namespace Database
{
    class Program
    {
        static void Main(string[] args)
        {
            BPlusTree<int, NodeStringInt> bt = new BPlusTree<int, NodeStringInt>(4);

            var list = new List<NodeStringInt>();

            for (int i = 1; i <= 6; i++)
            {
                if (i != 9999)
                {
                    var node = new NodeStringInt(i);
                    bt.Insert(node);
                    bt.InsertRepair(null,null);
                }
            }
            bt.TraverseForword(bt.Root, bt.TraverseOutput);

            Console.ReadKey();
        }
    }
}
