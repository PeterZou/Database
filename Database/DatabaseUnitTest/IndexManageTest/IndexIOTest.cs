using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using Database.IndexManage;
using Database.IndexManage.IndexValue;
using Database.Const;
using Database.RecordManage;
using System.IO;
using Database.FileManage;

namespace DatabaseUnitTest.IndexManageTest
{
    [TestClass]
    public class IndexIOTest
    {
        private Func<int, string> ConverIntToString = p => { string str = Convert.ToString(p); return str; };
        private Func<string, int> ConverStringToInt = p => { int num = 0; Int32.TryParse(p, out num); return num; };
        private Func<int> CreatNewTK = () => { return 0; };
        private Func<int, int> OccupiedNum = p => { return 4; };

        private NodeDisk<int> CreateNodeDisk()
        {
            NodeDisk<int> node = new NodeDisk<int>();
            node.length = 81;
            node.isLeaf = 0;
            node.capacity = 5;
            node.height = 2;
            node.keyList = new List<int> { 1, 2, 3, 4, 5 };
            node.childRidList = new List<RID> {
                new RID(1,2),
                new RID(3,4),
                new RID(5,6),
                new RID(7,8),
                new RID(9,10),
                new RID(11,12)
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


            hdr.rootRID = new RID(-1, -1);

            hdr.dicCount = 13;
            var dic = new Dictionary<int, int>();
            dic.Add(0, -1);
            dic.Add(1, -1);
            dic.Add(2, -1);
            dic.Add(3, -1);
            dic.Add(4, -1);
            dic.Add(5, -1);
            dic.Add(6, -1);
            dic.Add(-1, -1);
            dic.Add(-2, -1);
            dic.Add(-3, -1);
            dic.Add(-4, -1);
            dic.Add(-5, -1);
            dic.Add(-6, -1);
            hdr.dic = dic;
            return hdr;
        }

        [TestMethod]
        public void WriteTheIndexFileHdr()
        {
            var hdr = CreatIndexFileHdr();
            char[] data = IndexManagerUtil<int>.IndexFileHdrToCharArray(hdr, ConverIntToString);

            string filePath = @"D:\test.txt";

            using (FileStream fs = new FileStream(filePath, FileMode.Create, FileAccess.Write))
            {
                using (StreamWriter sw = new StreamWriter(fs))
                {
                    sw.Write(data);
                }
            }
        }

        [TestMethod]
        public void ReadTheIndexFileHdr()
        {
            string filePath = @"D:\test.txt";

            char[] data = new char[122];

            PF_FileHandle pfh = new PF_FileHandle();

            var d = IndexManagerUtil<int>.ReadIndexFileHdr(filePath, ConverStringToInt);
        }

        [TestMethod]
        public void SetNodeDiskToCharTest()
        {
            var node = CreateNodeDisk();
            char[] data = IndexManagerUtil<int>.SetNodeDiskToChar(node, ConverIntToString);
        }

        [TestMethod]
        public void CreateAndOpenIndexFileTestWithDegree4()
        {
            int treeDegree = 4;
            PF_Manager pfm = new PF_Manager();
            IX_Manager<int> ixm = new IX_Manager<int>(pfm, ConverIntToString,
                ConverStringToInt, CreatNewTK, OccupiedNum);

            ixm.CreateFile(@"D:\IndexFile.txt", 30, ConstProperty.AttrType.INT);

            IX_FileHandle<int> ifh = ixm.OpenFile(@"D:\IndexFile.txt", treeDegree);
            ifh.InsertEntry(1);
            ifh.FlushPages();
            ifh.InsertEntry(2);

            ifh.InsertEntry(3);
            ifh.InsertEntry(4);
            ifh.InsertEntry(5);

            // Branch test
            ifh.InsertEntry(6);
            ifh.InsertEntry(7);
            ifh.InsertEntry(8);
            ifh.InsertEntry(9);
            ifh.InsertEntry(10);
            ifh.InsertEntry(11);
            ifh.InsertEntry(12);
            ifh.FlushPages();
        }

        [TestMethod]
        public void CreateAndOpenIndexFileTestWithDegree5()
        {
            int treeDegree = 5;
            PF_Manager pfm = new PF_Manager();
            IX_Manager<int> ixm = new IX_Manager<int>(pfm, ConverIntToString,
                ConverStringToInt, CreatNewTK, OccupiedNum);

            ixm.CreateFile(@"D:\IndexFile.txt", 30, ConstProperty.AttrType.INT);

            IX_FileHandle<int> ifh = ixm.OpenFile(@"D:\IndexFile.txt", treeDegree);
            ifh.InsertEntry(1);
            ifh.FlushPages();
            for (int i = 2; i < 15; i++)
            {
                ifh.InsertEntry(i);
            }
            ifh.InsertEntry(15);
            ifh.FlushPages();
        }

        [TestMethod]
        public void CloseAndReOpenTestWithDegree4()
        {
            int treeDegree = 4;
            PF_Manager pfm = new PF_Manager();
            IX_Manager<int> ixm = new IX_Manager<int>(pfm, ConverIntToString,
                ConverStringToInt, CreatNewTK, OccupiedNum);

            IX_FileHandle<int> ifh = ixm.OpenFile(@"D:\IndexFile.txt", treeDegree);

            ifh.InsertEntry(13);
            ifh.InsertEntry(14);
            ifh.InsertEntry(15);
            ifh.InsertEntry(16);
            ifh.InsertEntry(17);

            ifh.FlushPages();
        }

        [TestMethod]
        public void StealFromLeftTest()
        {
            int treeDegree = 5;
            PF_Manager pfm = new PF_Manager();
            IX_Manager<int> ixm = new IX_Manager<int>(pfm, ConverIntToString,
                ConverStringToInt, CreatNewTK, OccupiedNum);

            ixm.CreateFile(@"D:\IndexFile.txt", 30, ConstProperty.AttrType.INT);

            IX_FileHandle<int> ifh = ixm.OpenFile(@"D:\IndexFile.txt", treeDegree);
            for (int i = 0; i < 13; i++)
            {
                ifh.InsertEntry(2*i+1);
            }
            ifh.InsertEntry(2);
            ifh.InsertEntry(6);
            ifh.DeleteEntry(5);
            ifh.DeleteEntry(6);
            ifh.FlushPages();
        }

        [TestMethod]
        public void StealFromRightTest()
        {
            int treeDegree = 5;
            PF_Manager pfm = new PF_Manager();
            IX_Manager<int> ixm = new IX_Manager<int>(pfm, ConverIntToString,
                ConverStringToInt, CreatNewTK, OccupiedNum);

            ixm.CreateFile(@"D:\IndexFile.txt", 30, ConstProperty.AttrType.INT);

            IX_FileHandle<int> ifh = ixm.OpenFile(@"D:\IndexFile.txt", treeDegree);
            for (int i = 0; i < 13; i++)
            {
                ifh.InsertEntry(2 * i + 1);
            }
            ifh.InsertEntry(10);
            ifh.DeleteEntry(7);
            ifh.FlushPages();
        }

        /// <summary>
        /// TODO
        /// </summary>
        [TestMethod]
        public void MergeTest()
        {
            int treeDegree = 5;
            PF_Manager pfm = new PF_Manager();
            IX_Manager<int> ixm = new IX_Manager<int>(pfm, ConverIntToString,
                ConverStringToInt, CreatNewTK, OccupiedNum);

            ixm.CreateFile(@"D:\IndexFile.txt", 30, ConstProperty.AttrType.INT);

            IX_FileHandle<int> ifh = ixm.OpenFile(@"D:\IndexFile.txt", treeDegree);
            for (int i = 0; i < 13; i++)
            {
                ifh.InsertEntry(2 * i + 1);
            }
            ifh.DeleteEntry(9);
            ifh.DeleteEntry(9);
        }

        [TestMethod]
        public void MergeTheLastNodeTest()
        {
        }
    }
}
