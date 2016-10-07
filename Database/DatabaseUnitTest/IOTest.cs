using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Database.BufferManage;
using Database.IO;
using System.Threading;
using System.IO;
using DatabaseUnitTest.IOTestFile;

namespace DatabaseUnitTest
{
    [TestClass]
    public class IOTest
    {
        [TestMethod]
        public void ReadTest()
        {
            IOFDDic.FDMapping.Add(1, @"D:\orders.data");
            PF_Buffermgr pf = new PF_Buffermgr(40);
            string s = pf.ReadPage(1,1);
        }

        [TestMethod]
        public void WirteTest()
        {
            IOFDDic.FDMapping.Add(1, @"D:\orders.data");
            PF_Buffermgr pf = new PF_Buffermgr(40);
            
            pf.WritePage(1,1,TestConst.ss);
        }

        /// <summary>
        /// Read the whole integer as a string with different char but the same bytes
        /// </summary>
        [TestMethod]
        public void ReadTheWholeInteger()
        {
            byte[] data = new byte[1000];
            string ioPath = @"D:\numString.txt";
            using (FileStream fs = new FileStream(ioPath, FileMode.Open))
            {
                using (BinaryReader sw = new BinaryReader(fs))
                {
                    sw.Read(data, 0, 66);
                }
            }

            var entry = Util.RawDataToObject<StructType>(data);
        }
    }
}
