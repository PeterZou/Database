using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Database.RecordManage;
using Database.FileManage;

namespace DatabaseUnitTest.RecordManageTest
{
    [TestClass]
    public class RecordTest
    {
        [TestMethod]
        public void WriteAndReadTest()
        {
            string filePath = @"D:\test.txt";
            var fh_m = new PF_Manager();
            RM_Manager rmm = new RM_Manager(fh_m);
            rmm.CreateFile(filePath,10,"Record file data, use for metadata".ToArray());
            var rh = rmm.OpenFile(filePath);

            // TODO
            //char[] chars = new char[10];
            //FileManagerUtil.ReplaceTheNextFree(chars, 10, 0, 10);
            string str = "123456789";
            rh.InsertRec(str.ToArray());

            rmm.CloseFile(rh);
        }

        [TestMethod]
        public void BitmapSet()
        {
            Bitmap b = new Bitmap(124);

            b.Reset();
            b.Set(90);
            bool flag = b.Test(90);
            flag = b.Test(91);

            Bitmap c = new Bitmap(b.bitArray, 124);
        }
    }
}
