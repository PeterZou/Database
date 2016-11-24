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

            // 1.one page insert
            char[] chars = new char[10];
            for (int i = 0; i < 405; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(chars, i, 0, 10);
                rh.InsertRec(chars);
            }

            for (int i = 0; i < 404; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(chars, i*10, 0, 10);
                rh.InsertRec(chars);
            }

            rmm.CloseFile(rh);

            rh = rmm.OpenFile(filePath);

            // 2.Mul pages insert
            chars = new char[10];
            for (int i = 0; i < 405; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(chars, i*100, 0, 10);
                rh.InsertRec(chars);
            }

            for (int i = 0; i < 405; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(chars, i*1000, 0, 10);
                rh.InsertRec(chars);
            }

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
