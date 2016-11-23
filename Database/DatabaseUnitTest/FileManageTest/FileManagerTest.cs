using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Database.FileManage;
using Database.Const;
using System.Reflection;

namespace DatabaseUnitTest
{
    [TestClass]
    public class FileManagerTest
    {
        /// <summary>
        /// Write to the disk
        /// </summary>
        [TestMethod]
        public void WriteTest()
        {
            string filePath = @"D:\test.txt";

            PF_Manager pf_m = new PF_Manager();
            pf_m.CreateFile(filePath);

            var fh = pf_m.OpenFile(filePath);

            // 1.Less than ConstProperty.PF_BUFFER_SIZE
            for (int i = 0; i < ConstProperty.PF_BUFFER_SIZE-1; i++)
            {
                var ph = fh.AllocatePage();
                ph.pPageData = FileManagerUtil.ModifiedPageData(ph,"zoujia");

                fh.SetThisPage(ph);
                fh.UnpinPage(i); 
            }
            fh.FlushPages();

            // 2.Dispose some pages
            fh.DisposePage(5);
            fh.DisposePage(38);
            fh.FlushPages();

            // 3.Reuse the dispose pages
            var ph1 = fh.AllocatePage();
            ph1.pPageData = FileManagerUtil.ModifiedPageData(ph1, "zoujiaReuseDispose0");
            fh.SetThisPage(ph1);
            fh.UnpinPage(ph1.pageNum);
            fh.FlushPages();

            ph1 = fh.AllocatePage();
            ph1.pPageData = FileManagerUtil.ModifiedPageData(ph1, "zoujiaReuseDispose1");
            fh.SetThisPage(ph1);
            fh.UnpinPage(ph1.pageNum);
            fh.FlushPages();

            ph1 = fh.AllocatePage();
            ph1.pPageData = FileManagerUtil.ModifiedPageData(ph1, "zoujiaReuseDispose2");
            fh.SetThisPage(ph1);
            fh.UnpinPage(ph1.pageNum);
            fh.FlushPages();

            // 4.More than ConstProperty.PF_BUFFER_SIZE
            ph1 = fh.AllocatePage();
            ph1.pPageData = FileManagerUtil.ModifiedPageData(ph1, "zoujiaReuseExceed0");
            fh.SetThisPage(ph1);
            fh.UnpinPage(ph1.pageNum);
            fh.FlushPages();

            ph1 = fh.AllocatePage();
            ph1.pPageData = FileManagerUtil.ModifiedPageData(ph1, "zoujiaReuseExceed1");
            fh.SetThisPage(ph1);
            fh.UnpinPage(ph1.pageNum);
            fh.FlushPages();

            fh.DisposePage(39);
            fh.DisposePage(8);
            
            fh.FlushPages();
        }

        /// <summary>
        /// Read from the disk
        /// </summary>
        [TestMethod]
        public void ReadTest()
        {
            PF_Manager pf_m = new PF_Manager();

            string filePath = @"D:\test.txt";

            var fh = pf_m.OpenFile(filePath);

            // Write to the disk
            char[] s = fh.GetThisPage(10).pPageData;
            string data = new string(s);

            data = new string(fh.GetNextPage(40).pPageData);

            data = new string(fh.GetPrevPage(1).pPageData);

            data = new string(fh.GetFirstPage().pPageData);

            data = new string(fh.GetLastPage().pPageData);
        }
    }
}
