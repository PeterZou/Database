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
            node.length = 97;
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
            node.leftRID = new RID(-1, -1);
            node.rightRID = new RID(-2, -2);

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

        private RIDKey<int> CreateRIDKey(int i)
        {
            return new RIDKey<int>(new RID(i, i), i);
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

            ixm.CreateFile(@"D:\IndexFile.txt", ConstProperty.AttrType.INT);

            IX_FileHandle<int> ifh = ixm.OpenFile(@"D:\IndexFile.txt", treeDegree);

            ifh.InsertEntry(CreateRIDKey(1));
            ifh.FlushPages();
            ifh.InsertEntry(CreateRIDKey(2));
            ifh.InsertEntry(CreateRIDKey(3));
            ifh.InsertEntry(CreateRIDKey(4));
            ifh.InsertEntry(CreateRIDKey(5));

            // Branch test
            ifh.InsertEntry(CreateRIDKey(6));
            ifh.InsertEntry(CreateRIDKey(7));
            ifh.InsertEntry(CreateRIDKey(8));
            ifh.InsertEntry(CreateRIDKey(9));
            ifh.InsertEntry(CreateRIDKey(10));
            ifh.InsertEntry(CreateRIDKey(11));
            ifh.InsertEntry(CreateRIDKey(12));
            ifh.FlushPages();
        }

        [TestMethod]
        public void CreateAndOpenIndexFileTestWithDegree5()
        {
            int treeDegree = 5;
            PF_Manager pfm = new PF_Manager();
            IX_Manager<int> ixm = new IX_Manager<int>(pfm, ConverIntToString,
                ConverStringToInt, CreatNewTK, OccupiedNum);

            ixm.CreateFile(@"D:\IndexFile.txt", ConstProperty.AttrType.INT);

            IX_FileHandle<int> ifh = ixm.OpenFile(@"D:\IndexFile.txt", treeDegree);
            ifh.InsertEntry(CreateRIDKey(1));
            ifh.FlushPages();
            for (int i = 2; i < 15; i++)
            {
                ifh.InsertEntry(CreateRIDKey(i));
            }
            ifh.InsertEntry(CreateRIDKey(15));
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

            ifh.InsertEntry(CreateRIDKey(13));
            ifh.InsertEntry(CreateRIDKey(14));
            ifh.InsertEntry(CreateRIDKey(15));
            ifh.InsertEntry(CreateRIDKey(16));
            ifh.InsertEntry(CreateRIDKey(17));

            ifh.FlushPages();
        }

        [TestMethod]
        public void StealFromLeftTest()
        {
            int treeDegree = 5;
            PF_Manager pfm = new PF_Manager();
            IX_Manager<int> ixm = new IX_Manager<int>(pfm, ConverIntToString,
                ConverStringToInt, CreatNewTK, OccupiedNum);

            ixm.CreateFile(@"D:\IndexFile.txt", ConstProperty.AttrType.INT);

            IX_FileHandle<int> ifh = ixm.OpenFile(@"D:\IndexFile.txt", treeDegree);
            for (int i = 0; i < 13; i++)
            {
                ifh.InsertEntry(CreateRIDKey(2 * i + 1));
            }
            ifh.InsertEntry(CreateRIDKey(2));
            ifh.InsertEntry(CreateRIDKey(6));
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

            ixm.CreateFile(@"D:\IndexFile.txt", ConstProperty.AttrType.INT);

            IX_FileHandle<int> ifh = ixm.OpenFile(@"D:\IndexFile.txt", treeDegree);
            for (int i = 0; i < 13; i++)
            {
                ifh.InsertEntry(CreateRIDKey(2 * i + 1));
            }
            ifh.InsertEntry(CreateRIDKey(10));
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

            ixm.CreateFile(@"D:\IndexFile.txt", ConstProperty.AttrType.INT);

            IX_FileHandle<int> ifh = ixm.OpenFile(@"D:\IndexFile.txt", treeDegree);
            for (int i = 0; i < 19; i++)
            {
                ifh.InsertEntry(CreateRIDKey(2 * i + 1));
            }
            ifh.DeleteEntry(27);
            ifh.FlushPages();
        }

        [TestMethod]
        public void MergeTest2()
        {
            int treeDegree = 5;
            PF_Manager pfm = new PF_Manager();
            IX_Manager<int> ixm = new IX_Manager<int>(pfm, ConverIntToString,
                ConverStringToInt, CreatNewTK, OccupiedNum);

            ixm.CreateFile(@"D:\IndexFile.txt", ConstProperty.AttrType.INT);

            IX_FileHandle<int> ifh = ixm.OpenFile(@"D:\IndexFile.txt", treeDegree);
            for (int i = 0; i < 13; i++)
            {
                ifh.InsertEntry(CreateRIDKey(2 * i + 1));
            }
            ifh.DeleteEntry(5);
            ifh.FlushPages();
        }

        [TestMethod]
        public void MergeTheLastRootNodeTest()
        {
            int treeDegree = 5;
            PF_Manager pfm = new PF_Manager();
            IX_Manager<int> ixm = new IX_Manager<int>(pfm, ConverIntToString,
                ConverStringToInt, CreatNewTK, OccupiedNum);

            ixm.CreateFile(@"D:\IndexFile.txt", ConstProperty.AttrType.INT);

            IX_FileHandle<int> ifh = ixm.OpenFile(@"D:\IndexFile.txt", treeDegree);
            for (int i = 1; i < 6; i++)
            {
                ifh.InsertEntry(CreateRIDKey(i));
            }
            ifh.DeleteEntry(4);
            ifh.DeleteEntry(5);
            ifh.FlushPages();
        }
    }
}
