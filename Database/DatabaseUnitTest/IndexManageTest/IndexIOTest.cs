using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.IndexManage;
using Database.IndexManage.IndexValue;
using Database.FileManage;
using Database.Const;
using Database.RecordManage;

namespace DatabaseUnitTest.IndexManageTest
{
    [TestClass]
    public class IndexIOTest
    {
        private Func<int, string> ConverIntToString = p => { string str = Convert.ToString(p); return str; };

        private NodeDisk<int> CreateNodeDisk()
        {
            NodeDisk<int> node = new NodeDisk<int>();
            node.length = 76;
            node.isLeaf = 0;
            node.capacity = 5;
            node.height = 2;
            node.keyList = new List<int> { 1, 2, 3, 4, 5 };
            node.childRidList = new List<RID> {
                new RID(1,5),
                new RID(12,45),
                new RID(12,45),
                new RID(12,45),
                new RID(12,45),
                new RID(12,45)
            };

            return node;
        }

        private IX_FileHdr<int> CreatIndexFileHdr()
        {
            IX_FileHdr<int> hdr = new IX_FileHdr<int>();
            hdr.firstFree = 3;
            hdr.numPages = 10;

            hdr.extRecordSize = 10;
            hdr.totalHeight = 6;
            hdr.indexType = ConstProperty.AttrType.INT;

            
            hdr.root = CreateNodeDisk();

            hdr.dicCount = 4;
            var dic = new Dictionary<int, int>();
            dic.Add(10, 120);
            dic.Add(3, 354);
            dic.Add(0, 384);
            dic.Add(13, 120);
            hdr.dic = dic;
            return hdr;
        }

        [TestMethod]
        public void WriteReadTheIndexFileHdr()
        {
            var hdr = CreatIndexFileHdr();
            char[] data = IndexManagerUtil<int>.WriteIndexFileHdr(hdr, ConverIntToString);
        }

        [TestMethod]
        public void SetNodeDiskToCharTest()
        {
            var node = CreateNodeDisk();
            char[] data = IndexManagerUtil<int>.SetNodeDiskToChar(node, ConverIntToString);
        }
    }
}
