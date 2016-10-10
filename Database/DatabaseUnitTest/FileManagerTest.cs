using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Database.FileManage;
using Database.Const;
using log4net;
using System.Reflection;

namespace DatabaseUnitTest
{
    [TestClass]
    public class FileManagerTest
    {
        private void MidMedthod(PF_Manager pf_m)
        {
            ILog m_log;
            Type type = MethodBase.GetCurrentMethod().DeclaringType;
            m_log = LogManager.GetLogger(type);

            string filePath = @"D:\test.txt";
            pf_m.CreateFile(filePath);

            var fh = pf_m.OpenFile(filePath);

            // Write to the disk
            int length = 0;
            for (int i = 0; i < 2 * ConstProperty.PF_BUFFER_SIZE; i++)
            {
                length++;
                if (length  == ConstProperty.PF_BUFFER_SIZE)
                {
                    fh.FlushPages();
                }
                var ph = fh.AllocatePage();

                if (i != ph.pageNum)
                {
                    m_log.Warn("Page number incorrect: " + ph.pageNum + " and " + i);
                }
                // Put only the page number into the page
                var note = fh.pf_bm.usedList.Where(node => node.pageNum == ph.pageNum).First();
                fh.UnpinPage(i);
            }
            fh.FlushPages();
        }

        /// <summary>
        /// Write to the disk
        /// </summary>
        [TestMethod]
        public void WriteTest()
        {

            ILog m_log;
            Type type = MethodBase.GetCurrentMethod().DeclaringType;
            m_log = LogManager.GetLogger(type);

            string filePath = @"D:\test.txt";

            PF_Manager pf_m = new PF_Manager();
            pf_m.CreateFile(filePath);

            var fh = pf_m.OpenFile(filePath);

            // Write to the disk
            int length = 0;
            for (int i = 0; i < 2 * ConstProperty.PF_BUFFER_SIZE; i++)
            {
                length++;
                if (length % ConstProperty.PF_BUFFER_SIZE == 0)
                {
                    fh.FlushPages();
                }
                var ph = fh.AllocatePage();

                if (i != ph.pageNum)
                {
                    m_log.Warn("Page number incorrect: " + ph.pageNum + " and " + i);
                }
                // Put only the page number into the page
                var note = fh.pf_bm.usedList.Where(node => node.pageNum == ph.pageNum).First();
                fh.UnpinPage(i);
            }
            fh.FlushPages();
        }

        /// <summary>
        /// Read from the disk
        /// </summary>
        [TestMethod]
        public void ReadTest()
        {
            PF_Manager pf_m = new PF_Manager();
            MidMedthod(pf_m);

            ILog m_log;
            Type type = MethodBase.GetCurrentMethod().DeclaringType;
            m_log = LogManager.GetLogger(type);

            string filePath = @"D:\test.txt";

            var fh = pf_m.OpenFile(filePath);

            // Write to the disk
            for (int i = 0; i < 2*ConstProperty.PF_BUFFER_SIZE; i++)
            {
                if (i == 56)
                {
                    fh.UnpinPage(i);
                }
            }
            fh.FlushPages();
        }
    }
}
