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
        List<RID> list = new List<RID>();

        [TestMethod]
        public void GUITest()
        {
            // input each num to test
            int numChar = 20;
            int numSlots = 201;

            string filePath = @"D:\test.txt";
            var fh_m = new PF_Manager();
            RM_Manager rmm = new RM_Manager(fh_m);
            rmm.CreateFile(filePath, numChar, "Record file data, use for metadata".ToArray());
            var rh = rmm.OpenFile(filePath);

            // Store all of the rid of record
            List<RID> ridList = new List<RID>();
            // 1.one page insert
            char[] chars = new char[numChar];
            for (int i = 1; i <= numSlots; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(chars, i, 0, numChar);
                ridList.Add(rh.InsertRec(chars));
            }

            GUTest(ridList, rh);

            for (int i = 1; i <= numSlots - 1; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(chars, i * 10, 0, numChar);
                ridList.Add(rh.InsertRec(chars));
            }

            GUTest(ridList, rh);

            rmm.CloseFile(rh);

            rh = rmm.OpenFile(filePath);

            // 2.Mul pages insert
            chars = new char[numChar];
            for (int i = 1; i <= numSlots; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(chars, i * 100, 0, numChar);
                ridList.Add(rh.InsertRec(chars));
            }

            // 3.换页对GetNextFreePage的影响
            GUTest(new RID(0, 5), rh);
            GUTest(ridList, rh);

            for (int i = 1; i <= numSlots; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(chars, i * 1000, 0, numChar);
                ridList.Add(rh.InsertRec(chars));
            }

            GUTest(ridList, rh);

            rmm.CloseFile(rh);
        }

        [TestMethod]
        public void DeleteTest()
        {
            string filePath = @"D:\test.txt";
            var fh_m = new PF_Manager();
            RM_Manager rmm = new RM_Manager(fh_m);
            rmm.CreateFile(filePath, 10, "Record file data, use for metadata".ToArray());
            var rh = rmm.OpenFile(filePath);

            // Store all of the rid of record
            List<RID> ridList = new List<RID>();
            // 1.one page insert
            char[] chars = new char[10];
            for (int i = 1; i <= 405; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(chars, i, 0, 10);
                ridList.Add(rh.InsertRec(chars));
            }

            var r = new Random();
            int num = r.Next(ridList.Count);
            // 获取当前页面与删除的页面的bitmap比较 TODO
            var rec = rh.GetRec(ridList[num]);
            
            int pageNum = ridList[num].Page;
            var bitmap = GetBitMap(rh, pageNum);

            rh.DeleteRec(ridList[num]);
            bitmap = GetBitMap(rh, pageNum);
            rmm.CloseFile(rh);
        }

        [TestMethod]
        public void DeleteTest2()
        {
            string filePath = @"D:\test.txt";
            var fh_m = new PF_Manager();
            RM_Manager rmm = new RM_Manager(fh_m);
            rmm.CreateFile(filePath,10,"Record file data, use for metadata".ToArray());
            var rh = rmm.OpenFile(filePath);

            // Store all of the rid of record
            List<RID> ridList = new List<RID>() {
                new RID(0,100),

                new RID(1, 28),
                new RID(0, 72),

                new RID(0, 6),
                new RID(2, 47)
            };
            int num = 0;

            // 1.one page insert
            char[] chars = new char[10];
            for (int i = 1; i <= 405; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(chars, i, 0, 10);
                rh.InsertRec(chars);
            }

            rh.DeleteRec(ridList[num]);
            num++;

            for (int i = 1; i <= 404; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(chars, i * 10, 0, 10);
                rh.InsertRec(chars);
            }

            rh.DeleteRec(ridList[num]);
            num++;
            rh.DeleteRec(ridList[num]);
            num++;

            rmm.CloseFile(rh);

            rh = rmm.OpenFile(filePath);

            // 2.Mul pages insert
            chars = new char[10];
            for (int i = 1; i <= 405; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(chars, i * 100, 0, 10);
                rh.InsertRec(chars);
            }

            // 3.换页对GetNextFreePage的影响
            rh.DeleteRec(ridList[num]);
            num++;
            rh.DeleteRec(ridList[num]);
            num++;

            for (int i = 1; i <= 405; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(chars, i * 1000+777, 0, 10);
                rh.InsertRec(chars);
            }

            //GUTest(ridList, rh);

            rmm.CloseFile(rh);

            rh = rmm.OpenFile(filePath);
            List<RM_Record> recList = new List<RM_Record>();
            foreach (var r in ridList)
            {
                recList.Add(rh.GetRec(r));
            }
            
            rmm.CloseFile(rh);
        }

        [TestMethod]
        public void InsertTest()
        {
            // input each num to test
            int numChar = 20;
            int numSlots = 201;

            string filePath = @"D:\test.txt";
            var fh_m = new PF_Manager();
            RM_Manager rmm = new RM_Manager(fh_m);
            
            rmm.CreateFile(filePath, numChar, "Record file data, use for metadata".ToArray());
            var rh = rmm.OpenFile(filePath);

            // Store all of the rid of record
            List<RID> ridList = new List<RID>();
            // 1.one page insert
            char[] chars = new char[numChar];
            for (int i = 1; i <= numSlots; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(chars, i, 0, numChar);
                ridList.Add(rh.InsertRec(chars));
            }
            for (int i = 1; i <= numSlots; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(chars, i * 10, 0, numChar);
                ridList.Add(rh.InsertRec(chars));
            }
            for (int i = 1; i <= numSlots; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(chars, i * 100, 0, numChar);
                ridList.Add(rh.InsertRec(chars));
            }
            for (int i = 1; i <= numSlots; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(chars, i * 1000, 0, numChar);
                ridList.Add(rh.InsertRec(chars));
            }
            for (int i = 1; i <= numSlots; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(chars, i * 10000, 0, numChar);
                ridList.Add(rh.InsertRec(chars));
            }

            rmm.CloseFile(rh);

            rh = rmm.OpenFile(filePath);
            GUTest(new RID(0, 5), rh);
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

        [TestMethod]
        public void EncodingSet()
        {
            char c1 = '\u0800';
            char c2 = '\u7000';
            char c3 = '\u7fff';

            string s1 = new string(Encoding.ASCII.GetChars(Encoding.ASCII.GetBytes(new char[] { c1, c2, c3 })));

            // 

            // 压缩
            string s2 = new string(Encoding.UTF8.GetChars(Encoding.UTF8.GetBytes(new char[] { c1,c2,c3 })));

            // 展开
            var s3 = s2.First();
        }

        private Bitmap GetBitMap(RM_FileHandle rh, int pageNum)
        {
            PF_PageHandle ph;
            ph = rh.pfHandle.GetThisPage(pageNum);
            rh.pfHandle.UnpinPage(pageNum);
            var pHdr = rh.GetPageHeader(ph);
            var bitmap = new Bitmap(pHdr.freeSlotMap, rh.GetNumSlots());

            return bitmap;
        }

        private void GUTest(List<RID> ridList, RM_FileHandle rh)
        {
            var r = new Random();
            int num = r.Next(ridList.Count);
            GUTest(ridList[num], rh);
        }

        private void GUTest(RID rid, RM_FileHandle rh)
        {
            var rec = rh.GetRec(rid);
            rec.data[0] = 'z';
            rh.UpdateRec(rec);
            list.Add(rid);
        }
    }
}
