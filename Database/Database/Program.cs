using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.BufferManage;
using log4net;
using log4net.Config;
using System.Reflection;
using Database.FileManage;
using Database.Const;

namespace Database
{
    class Program
    {
        static void Main(string[] args)
        {
            XmlConfigurator.Configure();

            ILog m_log;
            Type type = MethodBase.GetCurrentMethod().DeclaringType;
            m_log = LogManager.GetLogger(type);

            string filePath = @"D:\test.txt";

            PF_Manager pf_m = new PF_Manager();

            var fh = pf_m.OpenFile(filePath);

            // Write to the disk
            for (int i = 0; i < ConstProperty.PF_BUFFER_SIZE - 20; i++)
            {
                var ph = fh.GetThisPage(i);

            }

            Console.ReadKey();
        }
    }
}
