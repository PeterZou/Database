using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Database.BufferManage;

namespace DatabaseUnitTest
{
    [TestClass]
    public class BufferManagerTest
    {
        [TestMethod]
        public void FountTest()
        {
            PF_Hashtable bm = new PF_Hashtable(10);

            bm.Insert(1, 3, 12);
            bm.Insert(1, 7, 14);
            bm.Insert(1, 13, 12);

            int count = bm.Found(1, 13);
        }

        [TestMethod]
        public void DeleteTest1()
        {
            PF_Hashtable bm = new PF_Hashtable(10);

            bm.Insert(1, 3, 12);
            bm.Insert(1, 7, 14);
            bm.Insert(1, 13, 12);

            bm.Delete(1, 13);
            bm.Delete(1, 3);
        }

        [TestMethod]
        public void DeleteTest2()
        {
            PF_Hashtable bm = new PF_Hashtable(10);

            bm.Insert(1, 3, 12);
            bm.Insert(1, 7, 14);
            bm.Insert(1, 13, 12);

            bm.Delete(1, 3);
            bm.Delete(1, 13);
        }
    }
}
