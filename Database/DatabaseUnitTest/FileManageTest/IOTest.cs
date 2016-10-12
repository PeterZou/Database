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
using Database.FileManage;

namespace DatabaseUnitTest
{
    [TestClass]
    public class IOTest
    {
        [TestMethod]
        public void IOReadTest()
        {
            IOFDDic.FDMapping.Add(1, @"D:\orders.data");
            PF_Buffermgr pf = new PF_Buffermgr(40);
            //char[] s = pf.ReadPage(1,1);
        }

        [TestMethod]
        public void IOWirteTest()
        {
            IOFDDic.FDMapping.Add(1, @"D:\orders.data");
            PF_Buffermgr pf = new PF_Buffermgr(40);
            
            //pf.WritePage(1,1,TestConst.ss.ToArray());
        }

        /// <summary>
        /// Read the whole integer as a string with different char but the same bytes
        /// </summary>
        [TestMethod]
        public void ReplaceTheNextFreeTest()
        {
            int i = -1;

            PF_FileHandle f = new PF_FileHandle();

            PF_BufPageDesc pf = new PF_BufPageDesc();
            pf.data = "42  ".ToArray();

            Int32.TryParse(new string(pf.data),out i);

            FileManagerUtil.ReplaceTheNextFree(pf,423,0);

            Int32.TryParse(new string(pf.data), out i);
        }
    }
}
